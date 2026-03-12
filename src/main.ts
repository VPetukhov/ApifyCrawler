import { Actor, log } from 'apify';
import { PlaywrightCrawler, SessionError } from 'crawlee';
import { chromium, type BrowserContext, type Page, type Response } from 'playwright';
import type { ProxyInfo } from '@crawlee/core';

import {
    extractProfileFromDownloadedPage,
    dedupeTargets,
    extractProfileFromDomSnapshot,
    extractProfileFromHtml,
    extractProfileFromUnknown,
    extractPostFromDownloadedPage,
    extractPostFromUnknown,
    normalizeProfileUrl,
    normalizePostUrl,
    sanitizeProxyUrl,
    safeJsonParse,
    summarizeError,
    type DomSnapshot,
    type ExtractedProfile,
    type ExtractedPost,
    type NormalizedInstagramTarget,
    type NormalizedProfileTarget,
    type NormalizedPostTarget,
    type PostComment,
    type RecentPost,
} from './instagram.js';

type BrowserContextCookie = Parameters<BrowserContext['addCookies']>[0][number];

interface InputCookie extends Partial<BrowserContextCookie> {
    name: string;
    value: string;
}

interface ActorInput {
    profileUrls?: string[];
    postUrls?: string[];
    proxyConfiguration?: Record<string, unknown>;
    maxConcurrency?: number;
    requestTimeoutSecs?: number;
    navigationTimeoutSecs?: number;
    maxRequestRetries?: number;
    maxItems?: number;
    useSessionPool?: boolean;
    persistCookiesPerSession?: boolean;
    maxSessionRotations?: number;
    initialCookies?: InputCookie[];
    cookieString?: string;
    overrideUserAgent?: string;
    includeRecentPosts?: boolean;
    maxRecentPosts?: number;
    includeVisibleComments?: boolean;
    maxVisibleComments?: number;
    debugLogResponses?: boolean;
}

interface CapturedResponsePayload {
    url: string;
    status: number;
    payload: unknown;
}

interface PageState {
    responses: CapturedResponsePayload[];
    installedResponseListener: boolean;
}

interface VisibleCommentsLoadResult {
    status: 'skipped' | 'loaded' | 'no-visible-comments' | 'limited-by-login-wall';
    visibleCandidateCount: number;
    limitedByLoginWall: boolean;
    collectedComments?: PostComment[];
}

const pageStateMap = new WeakMap<Page, PageState>();
const BLOCK_STATUS_CODES = new Set([401, 403, 429]);
const TRANSIENT_RESOURCE_TYPES = new Set(['image', 'media', 'font']);

await Actor.init();

try {
    const input = await Actor.getInput<ActorInput>() ?? {};
    validateInput(input);

    const normalizedTargets = buildTargets(input);
    await Actor.setStatusMessage(`Preparing ${normalizedTargets.length} Instagram request(s).`);

    const proxyConfiguration = await Actor.createProxyConfiguration(input.proxyConfiguration);
    const includeRecentPosts = input.includeRecentPosts ?? true;
    const maxRecentPosts = input.maxRecentPosts ?? 12;
    const includeVisibleComments = input.includeVisibleComments ?? true;
    const maxVisibleComments = input.maxVisibleComments;
    const initialCookies = normalizeCookies(input.initialCookies, input.cookieString);

    const crawler = new PlaywrightCrawler({
        proxyConfiguration,
        maxConcurrency: input.maxConcurrency ?? 3,
        maxRequestRetries: input.maxRequestRetries ?? 6,
        maxSessionRotations: input.maxSessionRotations ?? 12,
        useSessionPool: input.useSessionPool ?? true,
        persistCookiesPerSession: input.persistCookiesPerSession ?? true,
        requestHandlerTimeoutSecs: input.requestTimeoutSecs ?? 90,
        navigationTimeoutSecs: input.navigationTimeoutSecs ?? 60,
        sameDomainDelaySecs: 2,
        sessionPoolOptions: {
            maxPoolSize: Math.max((input.maxConcurrency ?? 3) * 3, 12),
            sessionOptions: {
                maxUsageCount: 12,
                maxAgeSecs: 60 * 45,
                maxErrorScore: 2,
                errorScoreDecrement: 0.5,
            },
        },
        browserPoolOptions: {
            useFingerprints: !input.overrideUserAgent,
            maxOpenPagesPerBrowser: 1,
            retireBrowserAfterPageCount: 12,
            closeInactiveBrowserAfterSecs: 30,
            operationTimeoutSecs: 45,
            fingerprintOptions: {
                useFingerprintCache: true,
                fingerprintCacheSize: 10_000,
                fingerprintGeneratorOptions: {
                    browsers: [
                        { name: 'chrome', minVersion: 120, httpVersion: '2' },
                    ],
                    devices: ['desktop'],
                    operatingSystems: ['windows', 'macos', 'linux'],
                    locales: ['en-US', 'en'],
                },
            },
        },
        launchContext: {
            launcher: chromium,
            useChrome: true,
            useIncognitoPages: true,
            userAgent: input.overrideUserAgent,
            launchOptions: {
                headless: true,
                args: [
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--disable-features=IsolateOrigins,site-per-process',
                    '--lang=en-US,en',
                ],
            },
        },
        preNavigationHooks: [
            async ({ page, request, session }, gotoOptions) => {
                ensurePageState(page);
                installPageNetworkCapture(page, input.debugLogResponses ?? false);

                await page.setExtraHTTPHeaders({
                    'accept-language': 'en-US,en;q=0.9',
                });

                await page.route('**/*', async (route) => {
                    const resourceType = route.request().resourceType();
                    if (TRANSIENT_RESOURCE_TYPES.has(resourceType)) {
                        await route.abort();
                        return;
                    }

                    await route.continue();
                });

                if (initialCookies.length > 0) {
                    await page.context().addCookies(initialCookies);

                    if (session) {
                        session.setCookies(initialCookies, request.url);
                    }
                }

                gotoOptions.waitUntil = 'domcontentloaded';
                gotoOptions.timeout = (input.navigationTimeoutSecs ?? 60) * 1_000;
            },
        ],
        postNavigationHooks: [
            async ({ page, response, session, request, log: crawlerLog }) => {
                const mainStatus = response?.status();
                if (mainStatus && BLOCK_STATUS_CODES.has(mainStatus)) {
                    session?.retire();
                    throw new SessionError(`Instagram returned HTTP ${mainStatus} for ${request.url}.`);
                }

                await dismissInstagramLoginModal(page, crawlerLog);
                await dismissInstagramCookieBanner(page, crawlerLog);

                const currentUrl = page.url();
                if (currentUrl.includes('/accounts/login/') || currentUrl.includes('/challenge/')) {
                    session?.retire();
                    throw new SessionError(`Instagram redirected the request to a login or challenge page for ${request.url}.`);
                }

                const bodyText = (await page.locator('body').innerText().catch(() => '')).toLowerCase();
                if (
                    bodyText.includes('please wait a few minutes before you try again')
                    || bodyText.includes('try again later')
                    || bodyText.includes('suspicious activity')
                ) {
                    session?.retire();
                    throw new SessionError(`Instagram signaled throttling or blocking on ${request.url}.`);
                }

                if (bodyText.includes('log in to see photos and videos')) {
                    crawlerLog.warning(`Instagram displayed a login wall for ${request.url}. Cookies or a different proxy session may be required.`);
                }
            },
        ],
        requestHandler: async ({ page, request, response, session, proxyInfo, log: crawlerLog }) => {
            await page.waitForLoadState('networkidle', { timeout: 15_000 }).catch(() => undefined);
            await page.waitForTimeout(2_000);

            await dismissInstagramLoginModal(page, crawlerLog);
            await dismissInstagramCookieBanner(page, crawlerLog);

            const target = request.userData.target as NormalizedInstagramTarget;
            const pageState = ensurePageState(page);

            if (target.kind === 'post') {
                const visibleCommentsLoadResult = includeVisibleComments
                    ? await loadVisibleComments(page, maxVisibleComments, crawlerLog)
                    : {
                        status: 'skipped',
                        visibleCandidateCount: 0,
                        limitedByLoginWall: false,
                    } satisfies VisibleCommentsLoadResult;

                const extractedPost = await extractPost({
                    page,
                    pageState,
                    target,
                    includeVisibleComments,
                    maxVisibleComments,
                    prefetchedVisibleComments: visibleCommentsLoadResult.collectedComments,
                });

                if (!extractedPost) {
                    const failureHint = await detectFailureHint(page, response);
                    if (failureHint.retryAsSessionError) {
                        session?.retire();
                        throw new SessionError(failureHint.message);
                    }

                    throw new Error(failureHint.message);
                }

                const commentFetchRestricted = detectLoggedOutCommentRestriction(pageState.responses);
                const result = {
                    targetType: 'post',
                    inputUrl: target.originalUrl,
                    normalizedUrl: target.normalizedUrl,
                    loadedUrl: page.url(),
                    shortcode: extractedPost.shortcode,
                    mediaPath: extractedPost.mediaPath ?? target.mediaPath,
                    postUrl: extractedPost.url ?? target.normalizedUrl,
                    ownerUsername: extractedPost.ownerUsername,
                    ownerFullName: extractedPost.ownerFullName,
                    caption: extractedPost.caption,
                    likesCount: extractedPost.likesCount,
                    commentsCount: extractedPost.commentsCount,
                    viewsCount: extractedPost.viewsCount,
                    playCount: extractedPost.playCount,
                    isVideo: extractedPost.isVideo,
                    mediaType: extractedPost.mediaType,
                    displayUrl: extractedPost.displayUrl,
                    thumbnailUrl: extractedPost.thumbnailUrl,
                    videoUrl: extractedPost.videoUrl,
                    takenAtTimestamp: extractedPost.takenAtTimestamp,
                    locationName: extractedPost.locationName,
                    visibleComments: extractedPost.visibleComments,
                    visibleCommentsCount: extractedPost.visibleComments?.length ?? 0,
                    visibleCommentsStatus: visibleCommentsLoadResult.status,
                    visibleCommentsCandidatesSeen: visibleCommentsLoadResult.visibleCandidateCount,
                    commentFetchRestricted,
                    commentFetchRestrictionReason: commentFetchRestricted
                        ? 'unauthorized-logged-out-query'
                        : undefined,
                    loginWallDetected: visibleCommentsLoadResult.limitedByLoginWall,
                    extractionSource: extractedPost.source,
                    extractionPath: extractedPost.sourcePath,
                    extractionScore: extractedPost.score,
                    scrape: {
                        scrapedAt: new Date().toISOString(),
                        sessionId: session?.id,
                        requestRetryCount: request.retryCount,
                        sessionRotationCount: request.sessionRotationCount ?? 0,
                        httpStatus: response?.status(),
                        proxy: serializeProxyInfo(proxyInfo),
                    },
                };

                await Actor.pushData(result);
                crawlerLog.info(`Scraped Instagram post ${result.shortcode} from ${target.normalizedUrl} using ${result.extractionSource}.`);
                return;
            }

            const extractedProfile = await extractProfile({
                page,
                pageState,
                target,
                includeRecentPosts,
                maxRecentPosts,
            });

            if (!extractedProfile) {
                const failureHint = await detectFailureHint(page, response);
                if (failureHint.retryAsSessionError) {
                    session?.retire();
                    throw new SessionError(failureHint.message);
                }

                throw new Error(failureHint.message);
            }

            const result = {
                targetType: 'profile',
                inputUrl: target.originalUrl,
                normalizedUrl: target.normalizedUrl,
                loadedUrl: page.url(),
                username: extractedProfile.username,
                fullName: extractedProfile.fullName,
                biography: extractedProfile.biography,
                externalUrl: extractedProfile.externalUrl,
                profilePicUrl: extractedProfile.profilePicUrl,
                isPrivate: extractedProfile.isPrivate,
                isVerified: extractedProfile.isVerified,
                followersCount: extractedProfile.followersCount,
                followingCount: extractedProfile.followingCount,
                postsCount: extractedProfile.postsCount,
                categoryName: extractedProfile.categoryName,
                recentPosts: extractedProfile.recentPosts,
                extractionSource: extractedProfile.source,
                extractionPath: extractedProfile.sourcePath,
                extractionScore: extractedProfile.score,
                scrape: {
                    scrapedAt: new Date().toISOString(),
                    sessionId: session?.id,
                    requestRetryCount: request.retryCount,
                    sessionRotationCount: request.sessionRotationCount ?? 0,
                    httpStatus: response?.status(),
                    proxy: serializeProxyInfo(proxyInfo),
                },
            };

            await Actor.pushData(result);
            crawlerLog.info(`Scraped ${result.username} from ${target.normalizedUrl} using ${result.extractionSource}.`);
        },
        failedRequestHandler: async ({ request, proxyInfo }) => {
            const target = request.userData.target as NormalizedInstagramTarget | undefined;

            await Actor.pushData({
                targetType: target?.kind ?? 'unknown',
                inputUrl: target?.originalUrl ?? request.url,
                normalizedUrl: target?.normalizedUrl ?? request.url,
                loadedUrl: request.loadedUrl,
                succeeded: false,
                errorMessages: request.errorMessages,
                retryCount: request.retryCount,
                sessionRotationCount: request.sessionRotationCount ?? 0,
                scrape: {
                    failedAt: new Date().toISOString(),
                    proxy: serializeProxyInfo(proxyInfo),
                },
            });
        },
    });

    await Actor.setStatusMessage(`Running crawler for ${normalizedTargets.length} Instagram request(s).`);

    await crawler.run(
        normalizedTargets.map((target) => ({
            url: target.normalizedUrl,
            uniqueKey: `${target.kind}:${target.normalizedUrl}`,
            userData: {
                label: target.kind.toUpperCase(),
                target,
            },
        })),
    );

    await Actor.setStatusMessage(`Finished. Processed ${normalizedTargets.length} Instagram request(s).`);
} finally {
    await Actor.exit();
}

function validateInput(input: ActorInput): void {
    const hasProfileUrls = Array.isArray(input.profileUrls) && input.profileUrls.length > 0;
    const hasPostUrls = Array.isArray(input.postUrls) && input.postUrls.length > 0;

    if (!hasProfileUrls && !hasPostUrls) {
        throw new Error('Input must contain a non-empty "profileUrls" and/or "postUrls" array.');
    }
}

function buildTargets(input: ActorInput): NormalizedInstagramTarget[] {
    const normalizedTargets = dedupeTargets([
        ...(input.profileUrls ?? []).map((url) => normalizeProfileUrl(url)),
        ...(input.postUrls ?? []).map((url) => normalizePostUrl(url)),
    ]);
    const maxItems = input.maxItems ?? normalizedTargets.length;
    return normalizedTargets.slice(0, maxItems);
}

function normalizeCookies(cookies: InputCookie[] | undefined, cookieString: string | undefined): BrowserContextCookie[] {
    const mergedCookies = [
        ...(cookies ?? []),
        ...parseCookieString(cookieString),
    ];

    const normalizedCookies = mergedCookies.map((cookie) => {
        const normalizedCookie: BrowserContextCookie = {
            name: cookie.name,
            value: cookie.value,
            url: cookie.url,
            domain: cookie.url ? undefined : (cookie.domain ?? '.instagram.com'),
            path: cookie.url ? undefined : (cookie.path ?? '/'),
            expires: cookie.expires,
            secure: cookie.secure ?? true,
            httpOnly: cookie.httpOnly ?? false,
            sameSite: cookie.sameSite ?? 'Lax',
            partitionKey: cookie.partitionKey,
        };

        return normalizedCookie;
    });

    const deduped = new Map<string, BrowserContextCookie>();
    for (const cookie of normalizedCookies) {
        const key = `${cookie.name}:${cookie.domain ?? cookie.url ?? ''}:${cookie.path ?? ''}`;
        deduped.set(key, cookie);
    }

    return Array.from(deduped.values());
}

function parseCookieString(cookieString: string | undefined): InputCookie[] {
    if (!cookieString) {
        return [];
    }

    const parsedCookies: Array<InputCookie | null> = cookieString
        .split(';')
        .map((part) => part.trim())
        .filter(Boolean)
        .map((part) => {
            const separatorIndex = part.indexOf('=');
            if (separatorIndex <= 0) {
                return null;
            }

            const name = part.slice(0, separatorIndex).trim();
            const value = part.slice(separatorIndex + 1).trim();
            if (!name || !value) {
                return null;
            }

            return {
                name,
                value,
                domain: '.instagram.com',
                path: '/',
                secure: true,
                httpOnly: false,
                sameSite: 'Lax' as const,
            } satisfies InputCookie;
        })
        ;

    return parsedCookies.filter((cookie): cookie is InputCookie => cookie !== null);
}

function ensurePageState(page: Page): PageState {
    const existingState = pageStateMap.get(page);
    if (existingState) {
        return existingState;
    }

    const state: PageState = {
        responses: [],
        installedResponseListener: false,
    };

    pageStateMap.set(page, state);
    return state;
}

function installPageNetworkCapture(page: Page, debugLogResponses: boolean): void {
    const state = ensurePageState(page);
    if (state.installedResponseListener) {
        return;
    }

    state.installedResponseListener = true;

    page.on('response', async (response) => {
        try {
            if (!shouldInspectResponse(response)) {
                return;
            }

            const body = await response.text();
            if (!body || body.length > 5_000_000) {
                return;
            }

            if (!looksLikeRelevantPayload(body)) {
                return;
            }

            const parsed = safeJsonParse(body);
            if (!parsed) {
                return;
            }

            state.responses.push({
                url: response.url(),
                status: response.status(),
                payload: parsed,
            });

            if (debugLogResponses) {
                log.debug(`Captured Instagram JSON payload from ${response.url()}`);
            }
        } catch (error) {
            if (debugLogResponses) {
                log.debug(`Failed to inspect response ${response.url()}: ${summarizeError(error)}`);
            }
        }
    });
}

function shouldInspectResponse(response: Response): boolean {
    const url = response.url();
    if (!url.includes('instagram.com')) {
        return false;
    }

    const resourceType = response.request().resourceType();
    if (resourceType !== 'xhr' && resourceType !== 'fetch' && resourceType !== 'document') {
        return false;
    }

    const contentType = (response.headers()['content-type'] ?? '').toLowerCase();
    const hasJsonLikeContentType = contentType.includes('application/json')
        || contentType.includes('text/javascript')
        || contentType.includes('application/x-javascript')
        || contentType.includes('text/x-javascript');

    if (!hasJsonLikeContentType && resourceType !== 'document') {
        return false;
    }

    return /web_profile_info|graphql|ajax\/bz|api\/v1|profile|user|media|comment|post|reel/i.test(url);
}

function looksLikeRelevantPayload(body: string): boolean {
    return /username|full_name|biography|profile_pic_url|web_profile_info|follower_count|edge_followed_by|shortcode|comment_count|comments_count|edge_media_to_comment|edge_media_to_parent_comment|like_count|likes_count|display_url|video_url|content_text|threaded_comments|parent_comment_id|unauthorized logged out query/i.test(body);
}

function detectLoggedOutCommentRestriction(responses: CapturedResponsePayload[]): boolean {
    return responses.some((response) => containsUnauthorizedLoggedOutQuery(response.payload));
}

function containsUnauthorizedLoggedOutQuery(root: unknown): boolean {
    const visited = new WeakSet<object>();
    const stack: unknown[] = [root];
    let explored = 0;

    while (stack.length > 0 && explored < 25_000) {
        const current = stack.pop();
        explored += 1;

        if (typeof current === 'string') {
            if (current.toLowerCase().includes('unauthorized logged out query')) {
                return true;
            }

            continue;
        }

        if (!current || typeof current !== 'object') {
            continue;
        }

        if (visited.has(current)) {
            continue;
        }

        visited.add(current);

        if (Array.isArray(current)) {
            for (const value of current) {
                stack.push(value);
            }
            continue;
        }

        for (const value of Object.values(current)) {
            stack.push(value);
        }
    }

    return false;
}

async function extractProfile(options: {
    page: Page;
    pageState: PageState;
    target: NormalizedProfileTarget;
    includeRecentPosts: boolean;
    maxRecentPosts: number;
}) {
    const { page, pageState, target, includeRecentPosts, maxRecentPosts } = options;

    const candidates: ExtractedProfile[] = [];

    for (const responsePayload of pageState.responses.slice().reverse()) {
        const extracted = extractProfileFromUnknown(responsePayload.payload, {
            usernameHint: target.usernameHint,
            source: 'network-response',
            includeRecentPosts,
            maxRecentPosts,
        });

        if (extracted) {
            extracted.profile.sourcePath = responsePayload.url;
            candidates.push(extracted.profile);
        }
    }

    const domSnapshot = await createDomSnapshot(page);
    const downloadedPageExtracted = await extractProfileFromDownloadedPage(domSnapshot, {
        usernameHint: target.usernameHint,
        source: 'downloaded-page-metascraper',
        includeRecentPosts,
        maxRecentPosts,
    });
    if (downloadedPageExtracted) {
        candidates.push(downloadedPageExtracted);
    }

    const domExtracted = extractProfileFromDomSnapshot(domSnapshot, {
        usernameHint: target.usernameHint,
        source: 'dom-snapshot',
        includeRecentPosts,
        maxRecentPosts,
    });
    if (domExtracted) {
        candidates.push(domExtracted);
    }

    const htmlExtracted = extractProfileFromHtml(domSnapshot.html, {
        usernameHint: target.usernameHint,
        source: 'html-regex',
        includeRecentPosts,
        maxRecentPosts,
    });
    if (htmlExtracted) {
        candidates.push(htmlExtracted);
    }

    if (candidates.length === 0) {
        return null;
    }

    let mergedProfile = candidates[0];
    for (const candidate of candidates.slice(1)) {
        if (candidate.username !== mergedProfile.username) {
            continue;
        }

        mergedProfile = mergeExtractedProfiles(mergedProfile, candidate);
    }

    return mergedProfile;
}

async function extractPost(options: {
    page: Page;
    pageState: PageState;
    target: NormalizedPostTarget;
    includeVisibleComments: boolean;
    maxVisibleComments?: number;
    prefetchedVisibleComments?: PostComment[];
}) {
    const { page, pageState, target, includeVisibleComments, maxVisibleComments, prefetchedVisibleComments } = options;
    const candidates: ExtractedPost[] = [];

    for (const responsePayload of pageState.responses.slice().reverse()) {
        const extracted = extractPostFromUnknown(responsePayload.payload, {
            shortcodeHint: target.shortcodeHint,
            source: 'network-response',
            includeVisibleComments,
            maxVisibleComments,
        });

        if (extracted) {
            extracted.post.sourcePath = responsePayload.url;
            candidates.push(extracted.post);
        }
    }

    const domSnapshot = await createDomSnapshot(page);
    const downloadedPageExtracted = extractPostFromDownloadedPage(domSnapshot, {
        shortcodeHint: target.shortcodeHint,
        mediaPathHint: target.mediaPath,
        includeVisibleComments,
        maxVisibleComments,
        source: 'downloaded-page',
    });
    if (downloadedPageExtracted) {
        candidates.push(downloadedPageExtracted);
    }

    const bestNetworkCandidate = candidates[0];
    const domExtracted = await extractPostFromPage(page, {
        shortcodeHint: target.shortcodeHint,
        mediaPath: target.mediaPath,
        maxVisibleComments,
        ownerUsername: bestNetworkCandidate?.ownerUsername,
        caption: bestNetworkCandidate?.caption,
        prefetchedCommentItems: prefetchedVisibleComments,
    });
    if (domExtracted) {
        candidates.push(domExtracted);
    }

    if (candidates.length === 0) {
        return null;
    }

    let mergedPost = candidates[0];
    for (const candidate of candidates.slice(1)) {
        if (candidate.shortcode !== mergedPost.shortcode) {
            continue;
        }

        mergedPost = mergeExtractedPosts(mergedPost, candidate);
    }

    return mergedPost;
}

async function extractPostFromPage(
    page: Page,
    options: {
        shortcodeHint: string;
        mediaPath: NormalizedPostTarget['mediaPath'];
        maxVisibleComments?: number;
        ownerUsername?: string;
        caption?: string;
        prefetchedCommentItems?: PostComment[];
    },
): Promise<ExtractedPost | null> {
    const extractedCommentItems = await extractVisibleCommentsFromDom(page);
    const commentItems = mergeVisibleComments(options.prefetchedCommentItems, extractedCommentItems) ?? extractedCommentItems;

    return page.evaluate(({ shortcodeHint, mediaPath, maxVisibleComments, ownerUsername, caption, commentItems }) => {
        const reservedSegments = new Set([
            '',
            'accounts',
            'explore',
            'developer',
            'about',
            'privacy',
            'legal',
            'directory',
            'reel',
            'reels',
            'p',
            'stories',
            'tv',
        ]);
        const usernamePattern = /^[a-zA-Z0-9._]{1,30}$/;
        const shortCodePattern = /^[a-zA-Z0-9_-]{5,80}$/;

        const normalizeWhitespace = (value: string | null | undefined) => value?.replace(/\u00a0/g, ' ').replace(/\s+/g, ' ').trim() || '';
        const parseFlexibleNumber = (value: string | null | undefined) => {
            const normalized = normalizeWhitespace(value).replace(/,/g, '');
            if (!normalized) return undefined;

            const suffix = normalized.slice(-1).toUpperCase();
            const multiplier = suffix === 'K' ? 1_000 : suffix === 'M' ? 1_000_000 : suffix === 'B' ? 1_000_000_000 : 1;
            const numericPart = multiplier === 1 ? normalized : normalized.slice(0, -1);
            const parsed = Number(numericPart);
            return Number.isFinite(parsed) ? Math.round(parsed * multiplier) : undefined;
        };
        const extractCount = (text: string, patterns: RegExp[]) => {
            for (const pattern of patterns) {
                const match = text.match(pattern);
                if (match?.[1]) {
                    return parseFlexibleNumber(match[1]);
                }
            }

            return undefined;
        };
        const extractUsernameFromHref = (href: string | null) => {
            if (!href) return undefined;

            try {
                const parsed = new URL(href, window.location.origin);
                if (!parsed.hostname.includes('instagram.com')) {
                    return undefined;
                }

                const segment = parsed.pathname.split('/').filter(Boolean)[0]?.toLowerCase();
                if (!segment || reservedSegments.has(segment) || !usernamePattern.test(segment)) {
                    return undefined;
                }

                return segment;
            } catch {
                return undefined;
            }
        };
        const isVisible = (element: Element) => {
            if (!(element instanceof HTMLElement)) {
                return false;
            }

            const style = window.getComputedStyle(element);
            if (style.display === 'none' || style.visibility === 'hidden' || Number(style.opacity) === 0) {
                return false;
            }

            const rect = element.getBoundingClientRect();
            return rect.width > 0 && rect.height > 0;
        };
        const stripAuxiliaryText = (fullText: string, commentAuthor: string, timeText: string) => {
            let cleaned = normalizeWhitespace(fullText);
            if (!cleaned) {
                return '';
            }

            const removablePhrases = [
                'Reply',
                'Replies',
                'View replies',
                'View more replies',
                'Hide replies',
                'See translation',
                'Edited',
                'Pinned',
                'Like',
                'Unlike',
                'Liked',
            ];

            if (commentAuthor) {
                const loweredAuthor = commentAuthor.toLowerCase();
                if (cleaned.toLowerCase().startsWith(loweredAuthor)) {
                    cleaned = cleaned.slice(commentAuthor.length).trim();
                }
            }

            if (timeText) {
                cleaned = cleaned.replace(timeText, ' ');
            }

            for (const phrase of removablePhrases) {
                cleaned = cleaned.replace(new RegExp(`\\b${phrase.replace(/[.*+?^${}()|[\\]\\\\]/g, '\\$&')}\\b`, 'gi'), ' ');
            }

            cleaned = cleaned.replace(/\b\d[\d.,]*(?:\s*[KMB])?\s+likes?\b/gi, ' ');
            cleaned = cleaned.replace(/\b\d[\d.,]*(?:\s*[KMB])?\s+replies?\b/gi, ' ');
            cleaned = normalizeWhitespace(cleaned);
            return cleaned;
        };
        const extractShortcodeFromLocation = () => {
            const segments = window.location.pathname.split('/').filter(Boolean);
            const shortCode = segments[1];
            return shortCode && shortCodePattern.test(shortCode) ? shortCode : undefined;
        };

        const root = document.querySelector('article') ?? document.querySelector('main') ?? document.body;
        if (!root) {
            return null;
        }

        const rootText = normalizeWhitespace((root as HTMLElement).innerText || root.textContent || '');
        const authorAnchors = Array.from(root.querySelectorAll('header a[href], a[href]'))
            .filter(isVisible)
            .map((anchor) => {
                const username = extractUsernameFromHref(anchor.getAttribute('href'));
                return username ? { username, text: normalizeWhitespace(anchor.textContent), href: (anchor as HTMLAnchorElement).href } : null;
            })
            .filter((value): value is { username: string; text: string; href: string } => Boolean(value));

        const owner = authorAnchors[0];
        const rootImages = Array.from(root.querySelectorAll('img'))
            .filter(isVisible)
            .map((image) => ({
                src: image.getAttribute('src') || image.getAttribute('currentSrc'),
                area: image.clientWidth * image.clientHeight,
                alt: normalizeWhitespace(image.getAttribute('alt')),
            }))
            .filter((image): image is { src: string; area: number; alt: string } => Boolean(image.src))
            .sort((a, b) => {
                const avatarPenaltyA = /profile picture/i.test(a.alt) ? 1 : 0;
                const avatarPenaltyB = /profile picture/i.test(b.alt) ? 1 : 0;
                if (avatarPenaltyA !== avatarPenaltyB) {
                    return avatarPenaltyA - avatarPenaltyB;
                }

                return b.area - a.area;
            })
            .map((image) => image.src);
        const video = Array.from(root.querySelectorAll('video')).find((element) => isVisible(element));
        const timeElement = root.querySelector('time[datetime]') as HTMLTimeElement | null;
        const takenAtTimestamp = timeElement?.dateTime ? Math.floor(new Date(timeElement.dateTime).getTime() / 1_000) : undefined;
        const likesCount = extractCount(rootText, [
            /(?:^|\s)([0-9.,]+(?:\s*[KMB])?)\s+likes?\b/i,
            /and\s+([0-9.,]+(?:\s*[KMB])?)\s+others\b/i,
        ]);
        const commentsCount = extractCount(rootText, [
            /view all\s+([0-9.,]+(?:\s*[KMB])?)\s+comments?\b/i,
            /([0-9.,]+(?:\s*[KMB])?)\s+comments?\b/i,
        ]);

        let extractedCaption: string | undefined = normalizeWhitespace(caption) || undefined;
        const extractedOwnerUsername = owner?.username ?? ownerUsername;
        if (!extractedCaption && extractedOwnerUsername) {
            const ownerComment = commentItems.find((comment) => comment.username === extractedOwnerUsername);
            extractedCaption = ownerComment?.text;
        }

        const filteredComments = commentItems
            .filter((comment) => {
                if (!extractedCaption || !extractedOwnerUsername) {
                    return true;
                }

                return !(comment.username === extractedOwnerUsername && normalizeWhitespace(comment.text) === extractedCaption);
            })
            .slice(0, maxVisibleComments ?? commentItems.length);

        const shortcode = shortcodeHint || extractShortcodeFromLocation();
        if (!shortcode) {
            return null;
        }

        return {
            shortcode,
            url: `https://www.instagram.com/${mediaPath}/${shortcode}/`,
            mediaPath,
            ownerUsername: extractedOwnerUsername,
            caption: extractedCaption,
            likesCount,
            commentsCount,
            isVideo: Boolean(video),
            mediaType: video ? 'video' : 'image',
            displayUrl: rootImages[0],
            thumbnailUrl: rootImages[0],
            videoUrl: video instanceof HTMLVideoElement ? (video.currentSrc || video.src || undefined) : undefined,
            takenAtTimestamp,
            visibleComments: filteredComments,
            source: 'dom-visible-post',
            score: 40 + (filteredComments.length > 0 ? 10 : 0) + (likesCount !== undefined ? 5 : 0),
        };
    }, { ...options, commentItems });
}

async function extractVisibleCommentsFromDom(page: Page): Promise<PostComment[]> {
    return page.evaluate(() => {
        const reservedSegments = new Set([
            '',
            'accounts',
            'explore',
            'developer',
            'about',
            'privacy',
            'legal',
            'directory',
            'reel',
            'reels',
            'p',
            'stories',
            'tv',
        ]);
        const usernamePattern = /^[a-zA-Z0-9._]{1,30}$/;
        const normalizeWhitespace = (value: string | null | undefined) => value?.replace(/\u00a0/g, ' ').replace(/\s+/g, ' ').trim() || '';
        const parseFlexibleNumber = (value: string | null | undefined) => {
            const normalized = normalizeWhitespace(value).replace(/,/g, '');
            if (!normalized) return undefined;

            const suffix = normalized.slice(-1).toUpperCase();
            const multiplier = suffix === 'K' ? 1_000 : suffix === 'M' ? 1_000_000 : suffix === 'B' ? 1_000_000_000 : 1;
            const numericPart = multiplier === 1 ? normalized : normalized.slice(0, -1);
            const parsed = Number(numericPart);
            return Number.isFinite(parsed) ? Math.round(parsed * multiplier) : undefined;
        };
        const extractCount = (text: string, patterns: RegExp[]) => {
            for (const pattern of patterns) {
                const match = text.match(pattern);
                if (match?.[1]) {
                    return parseFlexibleNumber(match[1]);
                }
            }

            return undefined;
        };
        const extractUsernameFromHref = (href: string | null) => {
            if (!href) return undefined;

            try {
                const parsed = new URL(href, window.location.origin);
                if (!parsed.hostname.includes('instagram.com')) {
                    return undefined;
                }

                const segment = parsed.pathname.split('/').filter(Boolean)[0]?.toLowerCase();
                if (!segment || reservedSegments.has(segment) || !usernamePattern.test(segment)) {
                    return undefined;
                }

                return segment;
            } catch {
                return undefined;
            }
        };
        const extractCommentIdFromHref = (href: string | null) => {
            if (!href) return undefined;

            try {
                const parsed = new URL(href, window.location.origin);
                const match = parsed.pathname.match(/\/c\/([^/?#]+)/i);
                return match?.[1];
            } catch {
                return undefined;
            }
        };
        const isVisible = (element: Element | null | undefined): element is HTMLElement => {
            if (!(element instanceof HTMLElement)) {
                return false;
            }

            const style = window.getComputedStyle(element);
            if (style.display === 'none' || style.visibility === 'hidden' || Number(style.opacity) === 0) {
                return false;
            }

            const rect = element.getBoundingClientRect();
            return rect.width > 0 && rect.height > 0;
        };
        const hasActionSignal = (element: HTMLElement) => {
            const text = normalizeWhitespace(element.innerText || element.textContent);
            if (/\b(?:like|reply|view replies|view all\s+\d[\d.,]*(?:\s*[kmb])?\s+replies|hide replies|more replies|load more comments|see translation|edited|pinned)\b/i.test(text)) {
                return true;
            }

            return Array.from(element.querySelectorAll('button, [role="button"], a'))
                .some((control) => /\b(?:like|reply|view replies|more replies|load more comments)\b/i.test(normalizeWhitespace((control as HTMLElement).innerText || control.textContent)));
        };
        const stripAuxiliaryText = (fullText: string, commentAuthor: string, timeText: string) => {
            let cleaned = normalizeWhitespace(fullText);
            if (!cleaned) {
                return '';
            }

            if (commentAuthor) {
                cleaned = cleaned.replace(new RegExp(`^${commentAuthor.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i'), ' ');
            }

            if (timeText) {
                cleaned = cleaned.replace(timeText, ' ');
            }

            cleaned = cleaned
                .replace(/\bview all\s+\d[\d.,]*(?:\s*[KMB])?\s+replies\b/gi, ' ')
                .replace(/\bview more replies\b/gi, ' ')
                .replace(/\bview replies\b/gi, ' ')
                .replace(/\bhide replies\b/gi, ' ')
                .replace(/\bload more comments\b/gi, ' ')
                .replace(/\bload more replies\b/gi, ' ')
                .replace(/\bsee translation\b/gi, ' ')
                .replace(/\bedited\b/gi, ' ')
                .replace(/\bpinned\b/gi, ' ')
                .replace(/\bfollow\b/gi, ' ')
                .replace(/\blike\b/gi, ' ')
                .replace(/\breply\b/gi, ' ')
                .replace(/\b\d[\d.,]*(?:\s*[KMB])?\s+likes?\b/gi, ' ')
                .replace(/\b\d[\d.,]*(?:\s*[KMB])?\s+replies?\b/gi, ' ')
                .replace(/\blog in to like or comment\.?\b/gi, ' ');

            return normalizeWhitespace(cleaned);
        };
        const pickCommentAuthor = (anchors: Array<{ username: string; href: string; text: string; hasAvatar: boolean }>) => {
            return anchors.find((anchor) => anchor.text && anchor.text.toLowerCase() === anchor.username)
                ?? anchors.find((anchor) => anchor.hasAvatar)
                ?? anchors[0];
        };
        const findCommentRoot = (seed: Element, boundary: HTMLElement) => {
            let current: Element | null = seed;

            while (current && current !== boundary) {
                if (!isVisible(current)) {
                    current = current.parentElement;
                    continue;
                }

                const element = current;
                const rawText = normalizeWhitespace(element.innerText || element.textContent);
                if (!rawText || rawText.length < 8 || rawText.length > 1_200) {
                    current = element.parentElement;
                    continue;
                }

                const timeElement = Array.from(element.querySelectorAll('time')).find((candidate) => isVisible(candidate));
                if (!timeElement) {
                    current = element.parentElement;
                    continue;
                }

                const avatarCount = Array.from(element.querySelectorAll('img[alt*="profile picture" i]'))
                    .filter((image) => isVisible(image))
                    .length;
                if (avatarCount === 0 || avatarCount > 2) {
                    current = element.parentElement;
                    continue;
                }

                const authors = Array.from(element.querySelectorAll('a[href]'))
                    .map((anchor) => extractUsernameFromHref(anchor.getAttribute('href')))
                    .filter((value): value is string => Boolean(value));
                const uniqueAuthors = new Set(authors);
                if (uniqueAuthors.size === 0 || uniqueAuthors.size > 5) {
                    current = element.parentElement;
                    continue;
                }

                if (!hasActionSignal(element)) {
                    current = element.parentElement;
                    continue;
                }

                return element;
            }

            return null;
        };

        const boundary = document.querySelector('article') ?? document.querySelector('main') ?? document.body;
        if (!(boundary instanceof HTMLElement)) {
            return [];
        }

        const commentItems: PostComment[] = [];
        const seenKeys = new Set<string>();
        const seenRoots = new Set<HTMLElement>();
        const seedElements = Array.from(boundary.querySelectorAll('img[alt*="profile picture" i], time, a[href*="/c/"]'))
            .filter((element) => isVisible(element));

        for (const seed of seedElements) {
            const root = findCommentRoot(seed, boundary);
            if (!root || seenRoots.has(root)) {
                continue;
            }

            seenRoots.add(root);

            const anchors = Array.from(root.querySelectorAll('a[href]'))
                .map((anchor) => {
                    const username = extractUsernameFromHref(anchor.getAttribute('href'));
                    if (!username) {
                        return null;
                    }

                    return {
                        username,
                        href: (anchor as HTMLAnchorElement).href,
                        text: normalizeWhitespace(anchor.textContent),
                        hasAvatar: Boolean(anchor.querySelector('img[alt*="profile picture" i]')),
                    };
                })
                .filter((value): value is { username: string; href: string; text: string; hasAvatar: boolean } => Boolean(value));

            if (anchors.length === 0) {
                continue;
            }

            const commentAuthor = pickCommentAuthor(anchors);
            if (!commentAuthor) {
                continue;
            }

            const rawText = normalizeWhitespace(root.innerText || root.textContent);
            if (!rawText) {
                continue;
            }

            const timeElement = Array.from(root.querySelectorAll('time')).find((element) => isVisible(element)) as HTMLTimeElement | undefined;
            const timeText = normalizeWhitespace(timeElement?.innerText);
            const cleanedText = stripAuxiliaryText(rawText, commentAuthor.text || commentAuthor.username, timeText);
            if (!cleanedText || cleanedText.length < 2) {
                continue;
            }

            const commentId = anchors
                .map((anchor) => extractCommentIdFromHref(anchor.href))
                .find(Boolean);
            const takenAt = timeElement?.dateTime ? Math.floor(new Date(timeElement.dateTime).getTime() / 1_000) : undefined;
            const likeCount = extractCount(rawText, [/\b([0-9.,]+(?:\s*[KMB])?)\s+likes?\b/i]);
            const dedupeKey = commentId ?? `${commentAuthor.username}:${cleanedText}:${takenAt ?? ''}`;
            if (seenKeys.has(dedupeKey)) {
                continue;
            }

            seenKeys.add(dedupeKey);
            commentItems.push({
                id: commentId,
                username: commentAuthor.username,
                text: cleanedText,
                likeCount,
                takenAtTimestamp: takenAt,
                profileUrl: commentAuthor.href,
            });
        }

        return commentItems;
    });
}

async function loadVisibleComments(
    page: Page,
    maxVisibleComments: number | undefined,
    crawlerLog: { debug: (message: string) => void },
) : Promise<VisibleCommentsLoadResult> {
    await dismissInstagramCookieBanner(page, crawlerLog);

    const initialAccess = await inspectPostCommentAccess(page);
    if (initialAccess.limitedByLoginWall) {
        return {
            status: 'limited-by-login-wall',
            visibleCandidateCount: 0,
            limitedByLoginWall: true,
            collectedComments: [],
        };
    }

    let collectedComments = await extractVisibleCommentsFromDom(page);
    let previousCount = collectedComments.length;
    if (maxVisibleComments !== undefined && previousCount >= maxVisibleComments) {
        return {
            status: previousCount > 0 ? 'loaded' : 'no-visible-comments',
            visibleCandidateCount: previousCount,
            limitedByLoginWall: false,
            collectedComments,
        };
    }

    let stableRounds = 0;

    for (let iteration = 0; iteration < 10; iteration += 1) {
        await dismissInstagramLoginModal(page, crawlerLog);
        await dismissInstagramCookieBanner(page, crawlerLog);

        const access = await inspectPostCommentAccess(page);
        if (access.limitedByLoginWall) {
            return {
                status: 'limited-by-login-wall',
                visibleCandidateCount: previousCount,
                limitedByLoginWall: true,
                collectedComments,
            };
        }

        const clicked = await clickCommentExpansionControls(page, crawlerLog);
        const scrolled = await scrollCommentContainers(page);
        if (!clicked && !scrolled) {
            break;
        }

        await page.waitForTimeout(clicked ? 900 : 350);
        await page.waitForLoadState('networkidle', { timeout: 2_000 }).catch(() => undefined);

        const currentComments = await extractVisibleCommentsFromDom(page);
        collectedComments = mergeVisibleComments(collectedComments, currentComments) ?? collectedComments;
        const currentCount = collectedComments.length;
        if (maxVisibleComments !== undefined && currentCount >= maxVisibleComments) {
            return {
                status: 'loaded',
                visibleCandidateCount: currentCount,
                limitedByLoginWall: false,
                collectedComments,
            };
        }

        if (currentCount <= previousCount) {
            stableRounds += 1;
        } else {
            stableRounds = 0;
        }

        previousCount = Math.max(previousCount, currentCount);

        if (stableRounds >= 2) {
            break;
        }
    }

    return {
        status: previousCount > 0 ? 'loaded' : 'no-visible-comments',
        visibleCandidateCount: previousCount,
        limitedByLoginWall: false,
        collectedComments,
    };
}

async function inspectPostCommentAccess(page: Page): Promise<{ limitedByLoginWall: boolean }> {
    const hasVisiblePostContent = await page.evaluate(() => {
        const root = document.querySelector('article') ?? document.querySelector('main');
        if (!(root instanceof HTMLElement)) {
            return false;
        }

        const rect = root.getBoundingClientRect();
        if (rect.width === 0 || rect.height === 0) {
            return false;
        }

        const text = (root.innerText || root.textContent || '').toLowerCase();
        return Boolean(root.querySelector('img, video, time')) && (
            /load more comments|reply|like|comment/i.test(text)
            || text.length > 160
        );
    });

    if (hasVisiblePostContent) {
        return { limitedByLoginWall: false };
    }

    const bodyText = (await page.locator('body').innerText().catch(() => '')).toLowerCase();
    if (
        page.url().includes('/accounts/login/')
        || bodyText.includes('log in to see photos and videos')
        || bodyText.includes('sign up to see photos and videos')
        || bodyText.includes('see instagram photos and videos')
    ) {
        return { limitedByLoginWall: true };
    }

    const hasVisibleAuthDialog = await page
        .locator('div[role="dialog"]')
        .filter({ hasText: /log in|sign up/i })
        .first()
        .isVisible({ timeout: 500 })
        .catch(() => false);

    return { limitedByLoginWall: hasVisibleAuthDialog };
}

async function clickCommentExpansionControls(
    page: Page,
    crawlerLog: { debug: (message: string) => void },
): Promise<boolean> {
    let clicked = false;
    const matcher = /view all.*comments|more comments|load more comments/i;

    for (let attempt = 0; attempt < 3; attempt += 1) {
        const controls = page
            .locator('button, [role="button"], a')
            .filter({ hasText: matcher });
        const count = await controls.count().catch(() => 0);
        let clickedThisRound = false;

        for (let index = 0; index < Math.min(count, 10); index += 1) {
            const control = controls.nth(index);
            const isVisible = await control.isVisible({ timeout: 500 }).catch(() => false);
            if (!isVisible) {
                continue;
            }

            const label = await control.innerText().catch(() => '');

            let didClick = await control.click({ timeout: 1_500, force: true }).then(() => true).catch(() => false);
            if (!didClick) {
                didClick = await control
                    .evaluate((element) => {
                        if (!(element instanceof HTMLElement)) {
                            return false;
                        }

                        element.click();
                        return true;
                    })
                    .catch(() => false);
            }

            if (!didClick) {
                continue;
            }

            clicked = true;
            clickedThisRound = true;
            crawlerLog.debug(`Expanded Instagram comments via control: ${label.trim()}`);
            await page.waitForTimeout(700);
            break;
        }

        if (!clickedThisRound) {
            break;
        }
    }

    return clicked;
}

async function scrollCommentContainers(page: Page): Promise<boolean> {
    return page.evaluate(() => {
        const normalizeWhitespace = (value: string | null | undefined) => value?.replace(/\u00a0/g, ' ').replace(/\s+/g, ' ').trim() || '';
        const hasRenderableSize = (element: Element | null | undefined): element is HTMLElement => {
            if (!(element instanceof HTMLElement)) {
                return false;
            }

            const style = window.getComputedStyle(element);
            if (style.display === 'none' || style.visibility === 'hidden' || Number(style.opacity) === 0) {
                return false;
            }

            const rect = element.getBoundingClientRect();
            return rect.width > 0 && rect.height > 0;
        };
        const isScrollable = (element: HTMLElement) => {
            const style = window.getComputedStyle(element);
            return /(auto|scroll)/.test(`${style.overflowY} ${style.overflow}`) && element.scrollHeight > element.clientHeight + 32 && element.clientHeight >= 120;
        };
        const findScrollableAncestor = (start: Element | null | undefined) => {
            let current = start?.parentElement ?? null;
            while (current) {
                if (isScrollable(current)) {
                    return current;
                }

                current = current.parentElement;
            }

            return null;
        };
        const scoreScrollable = (element: HTMLElement) => {
            const rect = element.getBoundingClientRect();
            const text = normalizeWhitespace(element.innerText || element.textContent);
            const visibleTimes = Array.from(element.querySelectorAll('time'))
                .filter((candidate) => hasRenderableSize(candidate))
                .length;
            const visibleAvatars = Array.from(element.querySelectorAll('img[alt*="profile picture" i]'))
                .filter((candidate) => hasRenderableSize(candidate))
                .length;

            let score = 0;
            score += Math.min(visibleTimes, 12) * 3;
            score += Math.min(visibleAvatars, 12) * 2;
            if (/load more comments|reply|view replies|like/i.test(text)) {
                score += 24;
            }
            if (rect.height >= 180 && rect.height <= window.innerHeight * 0.9) {
                score += 12;
            }
            if (element.scrollHeight > element.clientHeight * 1.5) {
                score += 8;
            }
            if (element.clientHeight >= window.innerHeight * 0.85) {
                score -= 6;
            }
            return score;
        };

        const loadMoreControl = Array.from(document.querySelectorAll('button, [role="button"], a'))
            .find((element) => hasRenderableSize(element) && /load more comments|view all.*comments|more comments/i.test(normalizeWhitespace(element.textContent)));
        const preferredScrollBox = findScrollableAncestor(loadMoreControl);

        const candidateScrollBoxes = Array.from(document.querySelectorAll('article, main, section, div[role="dialog"], div'))
            .filter((element): element is HTMLElement => element instanceof HTMLElement)
            .filter(isScrollable)
            .sort((a, b) => scoreScrollable(b) - scoreScrollable(a));

        const target = preferredScrollBox ?? candidateScrollBoxes[0];
        if (target) {
            const before = target.scrollTop;
            const maxScrollTop = Math.max(target.scrollHeight - target.clientHeight, 0);
            const step = Math.max(Math.round(target.clientHeight * 0.82), 260);
            target.scrollTop = Math.min(before + step, maxScrollTop);
            target.dispatchEvent(new Event('scroll', { bubbles: true }));
            if (target.scrollTop !== before) {
                return true;
            }
        }

        const windowBefore = window.scrollY;
        window.scrollBy(0, Math.max(window.innerHeight * 0.8, 900));
        return window.scrollY !== windowBefore;
    });
}

async function createDomSnapshot(page: Page): Promise<DomSnapshot> {
    return page.evaluate(() => {
        const meta: Record<string, string> = {};
        for (const element of document.querySelectorAll('meta')) {
            const key = element.getAttribute('property') ?? element.getAttribute('name');
            const value = element.getAttribute('content');
            if (key && value) {
                meta[key] = value;
            }
        }

        const jsonLd = Array.from(document.querySelectorAll('script[type="application/ld+json"]'))
            .map((element) => element.textContent ?? '')
            .filter(Boolean);

        const header = document.querySelector('main header') ?? document.querySelector('header');
        const imageUrls = Array.from(document.images)
            .map((image) => image.currentSrc || image.src)
            .filter(Boolean)
            .slice(0, 8);

        return {
            title: document.title,
            url: window.location.href,
            meta,
            jsonLd,
            headerText: header?.textContent?.trim() ?? '',
            bodyText: document.body?.innerText ?? '',
            imageUrls,
            html: document.documentElement.outerHTML,
        };
    });
}

async function detectFailureHint(page: Page, response: Response | undefined): Promise<{ message: string; retryAsSessionError: boolean }> {
    const bodyText = (await page.locator('body').innerText().catch(() => '')).toLowerCase();
    const status = response?.status();

    if (status && BLOCK_STATUS_CODES.has(status)) {
        return {
            message: `Instagram returned HTTP ${status}.`,
            retryAsSessionError: true,
        };
    }

    if (page.url().includes('/accounts/login/') || bodyText.includes('log in to see photos and videos')) {
        return {
            message: 'Instagram displayed a login wall. Provide cookies or a different proxy session.',
            retryAsSessionError: true,
        };
    }

    if (
        bodyText.includes('please wait a few minutes before you try again')
        || bodyText.includes('try again later')
        || bodyText.includes('something went wrong')
    ) {
        return {
            message: 'Instagram returned a throttling or temporary failure page.',
            retryAsSessionError: true,
        };
    }

    return {
        message: `Could not extract Instagram data from ${page.url()}.`,
        retryAsSessionError: false,
    };
}

function serializeProxyInfo(proxyInfo: ProxyInfo | undefined) {
    if (!proxyInfo) {
        return undefined;
    }

    return {
        url: sanitizeProxyUrl(proxyInfo.url),
        hostname: proxyInfo.hostname,
        port: Number(proxyInfo.port),
        sessionId: proxyInfo.sessionId,
        proxyTier: proxyInfo.proxyTier,
    };
}

async function dismissInstagramLoginModal(page: Page, crawlerLog: { debug: (message: string) => void }): Promise<void> {
    const likelyLoginDialog = page
        .locator('div[role="dialog"]')
        .filter({ hasText: /log in|sign up/i })
        .first();

    const hasDialog = await likelyLoginDialog.isVisible({ timeout: 1_000 }).catch(() => false);
    if (!hasDialog) {
        return;
    }

    const closeSelectors = [
        'button:has-text("Not now")',
        'button:has-text("Not Now")',
        'button[aria-label="Close"]',
        '[role="button"][aria-label="Close"]',
        'svg[aria-label="Close"]',
    ];

    for (const selector of closeSelectors) {
        const closeTarget = page.locator(selector).first();
        const isVisible = await closeTarget.isVisible({ timeout: 800 }).catch(() => false);
        if (!isVisible) {
            continue;
        }

        const clicked = await closeTarget.click({ timeout: 1_500 }).then(() => true).catch(() => false);
        if (clicked) {
            await page.waitForTimeout(400);
            crawlerLog.debug(`Dismissed Instagram login popup via selector: ${selector}`);
            return;
        }
    }

    await page.keyboard.press('Escape').catch(() => undefined);
    await page.waitForTimeout(250);
}

async function dismissInstagramCookieBanner(page: Page, crawlerLog: { debug: (message: string) => void }): Promise<void> {
    const labels = [
        'Decline optional cookies',
        'Only allow essential cookies',
        'Allow all cookies',
        'Accept all',
        'Accept all cookies',
    ];

    for (const label of labels) {
        const button = page.getByRole('button', { name: label }).first();
        const isVisible = await button.isVisible({ timeout: 500 }).catch(() => false);
        if (!isVisible) {
            continue;
        }

        let clicked = await button.click({ timeout: 1_000, force: true }).then(() => true).catch(() => false);
        if (!clicked) {
            clicked = await button
                .evaluate((element) => {
                    if (!(element instanceof HTMLElement)) {
                        return false;
                    }

                    element.click();
                    return true;
                })
                .catch(() => false);
        }

        if (clicked) {
            await page.waitForTimeout(400);
            crawlerLog.debug(`Dismissed Instagram cookie banner via button: ${label}`);
            return;
        }
    }
}

function mergeExtractedProfiles(base: ExtractedProfile, candidate: ExtractedProfile): ExtractedProfile {
    const primary = pickRicherProfile(base, candidate);
    const secondary = primary === base ? candidate : base;

    return {
        username: primary.username,
        fullName: primary.fullName ?? secondary.fullName,
        biography: primary.biography ?? secondary.biography,
        externalUrl: primary.externalUrl ?? secondary.externalUrl,
        profilePicUrl: primary.profilePicUrl ?? secondary.profilePicUrl,
        isPrivate: primary.isPrivate ?? secondary.isPrivate,
        isVerified: primary.isVerified ?? secondary.isVerified,
        followersCount: primary.followersCount ?? secondary.followersCount,
        followingCount: primary.followingCount ?? secondary.followingCount,
        postsCount: primary.postsCount ?? secondary.postsCount,
        categoryName: primary.categoryName ?? secondary.categoryName,
        recentPosts: mergeRecentPosts(primary.recentPosts, secondary.recentPosts),
        source: primary.source,
        sourcePath: primary.sourcePath ?? secondary.sourcePath,
        score: Math.max(primary.score, secondary.score),
    };
}

function pickRicherProfile(a: ExtractedProfile, b: ExtractedProfile): ExtractedProfile {
    const aScore = profileRichnessScore(a);
    const bScore = profileRichnessScore(b);
    if (aScore === bScore) {
        return b.score > a.score ? b : a;
    }

    return bScore > aScore ? b : a;
}

function profileRichnessScore(profile: ExtractedProfile): number {
    let score = 0;
    if (profile.fullName) score += 1;
    if (profile.biography) score += 2;
    if (profile.externalUrl) score += 1;
    if (profile.profilePicUrl) score += 1;
    if (profile.followersCount !== undefined) score += 3;
    if (profile.followingCount !== undefined) score += 2;
    if (profile.postsCount !== undefined) score += 2;
    if (profile.categoryName) score += 1;
    if (profile.recentPosts?.length) score += Math.min(profile.recentPosts.length, 5);
    return score;
}

function mergeExtractedPosts(base: ExtractedPost, candidate: ExtractedPost): ExtractedPost {
    const primary = pickRicherPost(base, candidate);
    const secondary = primary === base ? candidate : base;

    return {
        shortcode: primary.shortcode,
        url: primary.url ?? secondary.url,
        mediaPath: primary.mediaPath ?? secondary.mediaPath,
        ownerUsername: primary.ownerUsername ?? secondary.ownerUsername,
        ownerFullName: primary.ownerFullName ?? secondary.ownerFullName,
        caption: primary.caption ?? secondary.caption,
        likesCount: primary.likesCount ?? secondary.likesCount,
        commentsCount: primary.commentsCount ?? secondary.commentsCount,
        viewsCount: primary.viewsCount ?? secondary.viewsCount,
        playCount: primary.playCount ?? secondary.playCount,
        isVideo: primary.isVideo ?? secondary.isVideo,
        mediaType: primary.mediaType ?? secondary.mediaType,
        displayUrl: primary.displayUrl ?? secondary.displayUrl,
        thumbnailUrl: primary.thumbnailUrl ?? secondary.thumbnailUrl,
        videoUrl: primary.videoUrl ?? secondary.videoUrl,
        takenAtTimestamp: primary.takenAtTimestamp ?? secondary.takenAtTimestamp,
        locationName: primary.locationName ?? secondary.locationName,
        visibleComments: mergeVisibleComments(primary.visibleComments, secondary.visibleComments),
        source: primary.source,
        sourcePath: primary.sourcePath ?? secondary.sourcePath,
        score: Math.max(primary.score, secondary.score),
    };
}

function pickRicherPost(a: ExtractedPost, b: ExtractedPost): ExtractedPost {
    const aScore = postRichnessScore(a);
    const bScore = postRichnessScore(b);
    if (aScore === bScore) {
        return b.score > a.score ? b : a;
    }

    return bScore > aScore ? b : a;
}

function postRichnessScore(post: ExtractedPost): number {
    let score = 0;
    if (post.ownerUsername) score += 2;
    if (post.caption) score += 2;
    if (post.likesCount !== undefined) score += 2;
    if (post.commentsCount !== undefined) score += 2;
    if (post.viewsCount !== undefined) score += 1;
    if (post.playCount !== undefined) score += 1;
    if (post.displayUrl) score += 2;
    if (post.videoUrl) score += 2;
    if (post.takenAtTimestamp !== undefined) score += 1;
    if (post.visibleComments?.length) score += Math.min(post.visibleComments.length, 8);
    return score;
}

function mergeVisibleComments(primary: PostComment[] | undefined, secondary: PostComment[] | undefined): PostComment[] | undefined {
    const mergedByKey = new Map<string, PostComment>();

    for (const comment of [...(primary ?? []), ...(secondary ?? [])]) {
        const key = comment.id ?? `${comment.username ?? 'unknown'}:${comment.text}:${comment.takenAtTimestamp ?? ''}`;
        const existing = mergedByKey.get(key);
        if (!existing) {
            mergedByKey.set(key, comment);
            continue;
        }

        mergedByKey.set(key, mergeVisibleComment(existing, comment));
    }

    const mergedComments = Array.from(mergedByKey.values());
    return mergedComments.length > 0 ? mergedComments : undefined;
}

function mergeVisibleComment(a: PostComment, b: PostComment): PostComment {
    const primary = visibleCommentRichnessScore(a) >= visibleCommentRichnessScore(b) ? a : b;
    const secondary = primary === a ? b : a;

    return {
        id: primary.id ?? secondary.id,
        username: primary.username ?? secondary.username,
        fullName: primary.fullName ?? secondary.fullName,
        text: primary.text || secondary.text,
        likeCount: primary.likeCount ?? secondary.likeCount,
        takenAtTimestamp: primary.takenAtTimestamp ?? secondary.takenAtTimestamp,
        profileUrl: primary.profileUrl ?? secondary.profileUrl,
        isReply: primary.isReply ?? secondary.isReply,
    };
}

function visibleCommentRichnessScore(comment: PostComment): number {
    let score = 0;
    if (comment.id) score += 2;
    if (comment.username) score += 2;
    if (comment.fullName) score += 1;
    if (comment.text) score += 2;
    if (comment.likeCount !== undefined) score += 1;
    if (comment.takenAtTimestamp !== undefined) score += 1;
    if (comment.profileUrl) score += 1;
    return score;
}

function mergeRecentPosts(primary: RecentPost[] | undefined, secondary: RecentPost[] | undefined): RecentPost[] | undefined {
    const mergedByKey = new Map<string, RecentPost>();

    for (const post of [...(primary ?? []), ...(secondary ?? [])]) {
        const key = post.shortcode ?? post.id ?? post.url ?? post.displayUrl;
        if (!key) {
            continue;
        }

        const existing = mergedByKey.get(key);
        if (!existing) {
            mergedByKey.set(key, post);
            continue;
        }

        mergedByKey.set(key, mergeRecentPost(existing, post));
    }

    const mergedPosts = Array.from(mergedByKey.values());
    return mergedPosts.length > 0 ? mergedPosts : undefined;
}

function mergeRecentPost(a: RecentPost, b: RecentPost): RecentPost {
    const primary = recentPostRichnessScore(a) >= recentPostRichnessScore(b) ? a : b;
    const secondary = primary === a ? b : a;

    return {
        id: primary.id ?? secondary.id,
        shortcode: primary.shortcode ?? secondary.shortcode,
        url: primary.url ?? secondary.url,
        caption: primary.caption ?? secondary.caption,
        commentsCount: primary.commentsCount ?? secondary.commentsCount,
        likesCount: primary.likesCount ?? secondary.likesCount,
        isVideo: primary.isVideo ?? secondary.isVideo,
        displayUrl: primary.displayUrl ?? secondary.displayUrl,
        thumbnailUrl: primary.thumbnailUrl ?? secondary.thumbnailUrl,
        videoUrl: primary.videoUrl ?? secondary.videoUrl,
        takenAtTimestamp: primary.takenAtTimestamp ?? secondary.takenAtTimestamp,
    };
}

function recentPostRichnessScore(post: RecentPost): number {
    let score = 0;
    if (post.shortcode) score += 3;
    if (post.displayUrl) score += 2;
    if (post.url) score += 2;
    if (post.caption) score += 1;
    if (post.likesCount !== undefined) score += 1;
    if (post.commentsCount !== undefined) score += 1;
    if (post.takenAtTimestamp !== undefined) score += 1;
    return score;
}
