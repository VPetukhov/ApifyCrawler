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
    normalizeProfileUrl,
    sanitizeProxyUrl,
    safeJsonParse,
    summarizeError,
    type DomSnapshot,
    type ExtractedProfile,
    type JsonObject,
    type NormalizedProfileTarget,
    type RecentPost,
} from './instagram.js';

type BrowserContextCookie = Parameters<BrowserContext['addCookies']>[0][number];

interface InputCookie extends Partial<BrowserContextCookie> {
    name: string;
    value: string;
}

interface ActorInput {
    profileUrls: string[];
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
    overrideUserAgent?: string;
    includeRecentPosts?: boolean;
    maxRecentPosts?: number;
    debugLogResponses?: boolean;
}

interface CapturedProfilePayload {
    url: string;
    status: number;
    payload: JsonObject;
}

interface PageState {
    responses: CapturedProfilePayload[];
    installedResponseListener: boolean;
}

const pageStateMap = new WeakMap<Page, PageState>();
const BLOCK_STATUS_CODES = new Set([401, 403, 429]);
const TRANSIENT_RESOURCE_TYPES = new Set(['image', 'media', 'font']);

await Actor.init();

try {
    const input = await Actor.getInput<ActorInput>() ?? { profileUrls: [] };
    validateInput(input);

    const normalizedTargets = buildTargets(input);
    await Actor.setStatusMessage(`Preparing ${normalizedTargets.length} Instagram profile request(s).`);

    const proxyConfiguration = await Actor.createProxyConfiguration(input.proxyConfiguration);
    const includeRecentPosts = input.includeRecentPosts ?? true;
    const maxRecentPosts = input.maxRecentPosts ?? 12;
    const initialCookies = normalizeCookies(input.initialCookies);

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
                installPageNetworkCapture(page, request.userData.usernameHint as string | undefined, input.debugLogResponses ?? false);

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

            const target = request.userData.target as NormalizedProfileTarget;
            const pageState = ensurePageState(page);

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
            const target = request.userData.target as NormalizedProfileTarget | undefined;

            await Actor.pushData({
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

    await Actor.setStatusMessage(`Running crawler for ${normalizedTargets.length} Instagram profile request(s).`);

    await crawler.run(
        normalizedTargets.map((target) => ({
            url: target.normalizedUrl,
            uniqueKey: target.normalizedUrl,
            userData: {
                label: 'PROFILE',
                usernameHint: target.usernameHint,
                target,
            },
        })),
    );

    await Actor.setStatusMessage(`Finished. Processed ${normalizedTargets.length} Instagram profile request(s).`);
} finally {
    await Actor.exit();
}

function validateInput(input: ActorInput): void {
    if (!Array.isArray(input.profileUrls) || input.profileUrls.length === 0) {
        throw new Error('Input must contain a non-empty "profileUrls" array.');
    }
}

function buildTargets(input: ActorInput): NormalizedProfileTarget[] {
    const normalizedTargets = dedupeTargets(input.profileUrls.map((url) => normalizeProfileUrl(url)));
    const maxItems = input.maxItems ?? normalizedTargets.length;
    return normalizedTargets.slice(0, maxItems);
}

function normalizeCookies(cookies: InputCookie[] | undefined): BrowserContextCookie[] {
    return (cookies ?? []).map((cookie) => {
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

function installPageNetworkCapture(page: Page, usernameHint: string | undefined, debugLogResponses: boolean): void {
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

            if (!looksLikeProfilePayload(body)) {
                return;
            }

            const parsed = safeJsonParse(body);
            if (!parsed) {
                return;
            }

            const extracted = extractProfileFromUnknown(parsed, {
                usernameHint,
                source: 'network-response',
                includeRecentPosts: true,
                maxRecentPosts: 12,
            });

            if (!extracted) {
                if (debugLogResponses) {
                    log.debug(`Skipped JSON response without a matching profile payload: ${response.url()}`);
                }

                return;
            }

            state.responses.push({
                url: response.url(),
                status: response.status(),
                payload: extracted.rawObject,
            });

            if (debugLogResponses) {
                log.debug(`Captured Instagram profile payload from ${response.url()}`);
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

    const contentType = response.headers()['content-type'] ?? '';
    if (!contentType.includes('application/json') && resourceType !== 'document') {
        return false;
    }

    return /web_profile_info|graphql|api\/v1|profile|user/i.test(url);
}

function looksLikeProfilePayload(body: string): boolean {
    return /username|full_name|biography|profile_pic_url|web_profile_info|follower_count|edge_followed_by/i.test(body);
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
        message: `Could not extract a profile payload from ${page.url()}.`,
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
