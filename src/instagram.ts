import metascraper from 'metascraper';
import metascraperDescription from 'metascraper-description';
import metascraperImage from 'metascraper-image';
import metascraperInstagram from 'metascraper-instagram';
import metascraperTitle from 'metascraper-title';
import metascraperUrl from 'metascraper-url';

type JsonValue = string | number | boolean | null | JsonObject | JsonValue[];

export interface JsonObject {
    [key: string]: JsonValue | undefined;
}

export type PostPathSegment = 'p' | 'reel' | 'reels' | 'tv';

export interface NormalizedProfileTarget {
    kind: 'profile';
    originalUrl: string;
    normalizedUrl: string;
    usernameHint: string;
}

export interface NormalizedPostTarget {
    kind: 'post';
    originalUrl: string;
    normalizedUrl: string;
    shortcodeHint: string;
    mediaPath: PostPathSegment;
}

export type NormalizedInstagramTarget = NormalizedProfileTarget | NormalizedPostTarget;

export interface RecentPost {
    id?: string;
    shortcode?: string;
    url?: string;
    caption?: string;
    commentsCount?: number;
    likesCount?: number;
    isVideo?: boolean;
    displayUrl?: string;
    thumbnailUrl?: string;
    videoUrl?: string;
    takenAtTimestamp?: number;
}

export interface ExtractedProfile {
    username: string;
    fullName?: string;
    biography?: string;
    externalUrl?: string;
    profilePicUrl?: string;
    isPrivate?: boolean;
    isVerified?: boolean;
    followersCount?: number;
    followingCount?: number;
    postsCount?: number;
    categoryName?: string;
    recentPosts?: RecentPost[];
    source: string;
    sourcePath?: string;
    score: number;
}

export interface PostComment {
    id?: string;
    username?: string;
    fullName?: string;
    text: string;
    likeCount?: number;
    takenAtTimestamp?: number;
    profileUrl?: string;
    isReply?: boolean;
}

export interface ExtractedPost {
    shortcode: string;
    url?: string;
    mediaPath?: PostPathSegment;
    ownerUsername?: string;
    ownerFullName?: string;
    caption?: string;
    likesCount?: number;
    commentsCount?: number;
    viewsCount?: number;
    playCount?: number;
    isVideo?: boolean;
    mediaType?: string;
    displayUrl?: string;
    thumbnailUrl?: string;
    videoUrl?: string;
    takenAtTimestamp?: number;
    locationName?: string;
    visibleComments?: PostComment[];
    source: string;
    sourcePath?: string;
    score: number;
}

export interface DomSnapshot {
    title: string;
    url: string;
    meta: Record<string, string>;
    jsonLd: string[];
    headerText: string;
    bodyText: string;
    imageUrls: string[];
    html: string;
}

interface ProfileSearchResult {
    profile: ExtractedProfile;
    rawObject: JsonObject;
}

interface PostSearchResult {
    post: ExtractedPost;
    rawObject: JsonObject;
}

const scrapeInstagramMetadata = metascraper([
    metascraperInstagram(),
    metascraperTitle(),
    metascraperDescription({ truncateLength: Number.MAX_SAFE_INTEGER }),
    metascraperImage(),
    metascraperUrl(),
]);

const POST_PATH_SEGMENTS = new Set<PostPathSegment>(['p', 'reel', 'reels', 'tv']);
const RESERVED_PATH_SEGMENTS = new Set([
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

const USERNAME_PATTERN = /^[a-zA-Z0-9._]{1,30}$/;
const SHORTCODE_PATTERN = /^[a-zA-Z0-9_-]{5,80}$/;

export function normalizeProfileUrl(rawUrl: string): NormalizedProfileTarget {
    const candidate = rawUrl.trim();
    if (!candidate) {
        throw new Error('Encountered an empty profile URL.');
    }

    const withProtocol = /^https?:\/\//i.test(candidate) ? candidate : `https://${candidate}`;
    const parsed = new URL(withProtocol);
    const hostname = parsed.hostname.replace(/^www\./, '').toLowerCase();

    if (hostname !== 'instagram.com') {
        throw new Error(`Unsupported hostname "${parsed.hostname}". Only instagram.com profile URLs are accepted.`);
    }

    const segments = parsed.pathname
        .split('/')
        .map((segment) => segment.trim())
        .filter(Boolean);

    let username = segments[0] ?? '';
    if (username === '_u') {
        username = segments[1] ?? '';
    }

    if (!username || RESERVED_PATH_SEGMENTS.has(username.toLowerCase()) || !USERNAME_PATTERN.test(username)) {
        throw new Error(`Could not infer an Instagram username from "${rawUrl}".`);
    }

    const normalizedUrl = `https://www.instagram.com/${username}/`;

    return {
        kind: 'profile',
        originalUrl: rawUrl,
        normalizedUrl,
        usernameHint: username.toLowerCase(),
    };
}

export function normalizePostUrl(rawUrl: string): NormalizedPostTarget {
    const candidate = rawUrl.trim();
    if (!candidate) {
        throw new Error('Encountered an empty post URL.');
    }

    const withProtocol = /^https?:\/\//i.test(candidate) ? candidate : `https://${candidate}`;
    const parsed = new URL(withProtocol);
    const hostname = parsed.hostname.replace(/^www\./, '').toLowerCase();

    if (hostname !== 'instagram.com') {
        throw new Error(`Unsupported hostname "${parsed.hostname}". Only instagram.com post URLs are accepted.`);
    }

    const segments = parsed.pathname
        .split('/')
        .map((segment) => segment.trim())
        .filter(Boolean);

    let mediaPath = segments[0]?.toLowerCase();
    let shortcode = segments[1] ?? '';

    if (mediaPath === '_u') {
        mediaPath = segments[1]?.toLowerCase();
        shortcode = segments[2] ?? '';
    }

    if (!mediaPath || !POST_PATH_SEGMENTS.has(mediaPath as PostPathSegment) || !shortcode || !SHORTCODE_PATTERN.test(shortcode)) {
        throw new Error(`Could not infer an Instagram post shortcode from "${rawUrl}".`);
    }

    const normalizedMediaPath = mediaPath as PostPathSegment;

    return {
        kind: 'post',
        originalUrl: rawUrl,
        normalizedUrl: `https://www.instagram.com/${normalizedMediaPath}/${shortcode}/`,
        shortcodeHint: shortcode,
        mediaPath: normalizedMediaPath,
    };
}

export function dedupeTargets<T extends { kind: string; normalizedUrl: string }>(targets: T[]): T[] {
    const seen = new Set<string>();
    const uniqueTargets: T[] = [];

    for (const target of targets) {
        const dedupeKey = `${target.kind}:${target.normalizedUrl}`;
        if (seen.has(dedupeKey)) {
            continue;
        }

        seen.add(dedupeKey);
        uniqueTargets.push(target);
    }

    return uniqueTargets;
}

export function safeJsonParse(value: string): unknown {
    try {
        return JSON.parse(value);
    } catch {
        return null;
    }
}

export function sanitizeProxyUrl(proxyUrl: string): string {
    try {
        const parsed = new URL(proxyUrl);
        if (parsed.password) {
            parsed.password = '***';
        }

        return parsed.toString();
    } catch {
        return proxyUrl;
    }
}

export function summarizeError(error: unknown): string {
    if (error instanceof Error) {
        return error.message;
    }

    return String(error);
}

export function extractProfileFromUnknown(
    root: unknown,
    options: {
        usernameHint?: string;
        includeRecentPosts?: boolean;
        maxRecentPosts?: number;
        source: string;
    },
): ProfileSearchResult | null {
    const best = findBestProfileCandidate(root, options.usernameHint);
    if (!best) {
        return null;
    }

    const profile = normalizeProfile(best.rawObject, {
        source: options.source,
        sourcePath: best.path,
        usernameHint: options.usernameHint,
        score: best.score,
    });

    if (!profile) {
        return null;
    }

    if (options.includeRecentPosts) {
        const recentPosts = extractRecentPosts(best.rawObject, options.maxRecentPosts ?? 12, options.usernameHint);
        if (recentPosts.length > 0) {
            profile.recentPosts = recentPosts;
        }
    }

    return {
        profile,
        rawObject: best.rawObject,
    };
}

export function extractPostFromUnknown(
    root: unknown,
    options: {
        shortcodeHint?: string;
        source: string;
        includeVisibleComments?: boolean;
        maxVisibleComments?: number;
    },
): PostSearchResult | null {
    const best = findBestPostCandidate(root, options.shortcodeHint);
    if (!best) {
        return null;
    }

    const post = normalizePost(best.rawObject, {
        source: options.source,
        sourcePath: best.path,
        shortcodeHint: options.shortcodeHint,
        score: best.score,
    });

    if (!post) {
        return null;
    }

    if (options.includeVisibleComments) {
        const visibleComments = extractVisibleCommentsFromUnknown(root, options.maxVisibleComments, {
            ownerUsername: post.ownerUsername,
            caption: post.caption,
        });
        if (visibleComments.length > 0) {
            post.visibleComments = visibleComments;
        }
    }

    return {
        post,
        rawObject: best.rawObject,
    };
}

export function extractProfileFromDomSnapshot(
    snapshot: DomSnapshot,
    options: {
        usernameHint?: string;
        includeRecentPosts?: boolean;
        maxRecentPosts?: number;
        source: string;
    },
): ExtractedProfile | null {
    const combinedDescription = [
        snapshot.meta.description,
        snapshot.meta['og:description'],
        snapshot.meta['twitter:description'],
        snapshot.headerText,
        snapshot.bodyText.slice(0, 3000),
    ]
        .filter(Boolean)
        .join('\n');

    const username = options.usernameHint
        ?? extractUsernameFromText(snapshot.title)
        ?? extractUsernameFromText(combinedDescription)
        ?? extractUsernameFromUrl(snapshot.url);

    if (!username) {
        return null;
    }

    const counts = extractCountsFromText(combinedDescription);
    const titleSource = snapshot.meta['og:title'] || snapshot.meta.title || snapshot.title;
    const fullName = extractFullName(titleSource, username);
    const biography = extractBiography(combinedDescription, username, fullName);
    const externalUrl = snapshot.meta['al:ios:url'] ?? snapshot.meta['twitter:url'];
    const profilePicUrl = snapshot.meta['og:image'] ?? snapshot.imageUrls[0];
    const loweredBody = snapshot.bodyText.toLowerCase();
    const isPrivate = loweredBody.includes('this account is private') || loweredBody.includes('private account');
    const isVerified = loweredBody.includes('verified') || /verified/i.test(snapshot.headerText);

    const profile: ExtractedProfile = {
        username,
        fullName,
        biography,
        externalUrl,
        profilePicUrl,
        isPrivate,
        isVerified,
        followersCount: counts.followersCount,
        followingCount: counts.followingCount,
        postsCount: counts.postsCount,
        source: options.source,
        score: 40 + (counts.followersCount !== undefined ? 10 : 0),
    };

    if (options.includeRecentPosts) {
        const htmlProfile = extractProfileFromHtml(snapshot.html, options);
        if (htmlProfile?.recentPosts?.length) {
            profile.recentPosts = htmlProfile.recentPosts.slice(0, options.maxRecentPosts ?? 12);
        }
    }

    return profile;
}

export async function extractProfileFromDownloadedPage(
    snapshot: DomSnapshot,
    options: {
        usernameHint?: string;
        includeRecentPosts?: boolean;
        maxRecentPosts?: number;
        source: string;
    },
): Promise<ExtractedProfile | null> {
    const metadata = await scrapeInstagramMetadata({
        html: snapshot.html,
        url: snapshot.url,
        pickPropNames: new Set(['author', 'description', 'image', 'title', 'url']),
    });

    const combinedDescription = [
        metadata.description,
        snapshot.headerText,
        snapshot.bodyText.slice(0, 3000),
    ]
        .filter(Boolean)
        .join('\n');

    const username = options.usernameHint
        ?? extractUsernameFromText(metadata.title ?? '')
        ?? extractUsernameFromText(metadata.description ?? '')
        ?? extractUsernameFromUrl(metadata.url ?? snapshot.url);

    if (!username) {
        return null;
    }

    const counts = extractCountsFromText(combinedDescription);
    const fullName = extractFullName(
        metadata.author ?? metadata.title ?? snapshot.title,
        username,
    ) ?? metadata.author;
    const biography = extractEscapedString(snapshot.html, 'biography')
        ?? extractBiography(combinedDescription, username, fullName);
    const externalUrl = extractEscapedString(snapshot.html, 'external_url')
        ?? snapshot.meta['al:ios:url']
        ?? snapshot.meta['twitter:url'];
    const profilePicUrl = metadata.image ?? snapshot.meta['og:image'] ?? snapshot.imageUrls[0];
    const loweredBody = snapshot.bodyText.toLowerCase();
    const isPrivate = loweredBody.includes('this account is private') || loweredBody.includes('private account');
    const isVerified = loweredBody.includes('verified') || /verified/i.test(snapshot.headerText);

    const profile: ExtractedProfile = {
        username,
        fullName,
        biography,
        externalUrl,
        profilePicUrl,
        isPrivate,
        isVerified,
        followersCount: counts.followersCount,
        followingCount: counts.followingCount,
        postsCount: counts.postsCount,
        source: options.source,
        score: 55 + (counts.followersCount !== undefined ? 10 : 0),
    };

    if (options.includeRecentPosts) {
        const htmlProfile = extractProfileFromHtml(snapshot.html, options);
        if (htmlProfile?.recentPosts?.length) {
            profile.recentPosts = htmlProfile.recentPosts.slice(0, options.maxRecentPosts ?? 12);
        }
    }

    return profile;
}

export function extractPostFromDownloadedPage(
    snapshot: DomSnapshot,
    options: {
        shortcodeHint?: string;
        mediaPathHint?: PostPathSegment;
        includeVisibleComments?: boolean;
        maxVisibleComments?: number;
        source: string;
    },
): ExtractedPost | null {
    const candidates: ExtractedPost[] = [];

    const embeddedPayloads = extractEmbeddedJsonPayloads(snapshot.html, snapshot.jsonLd);
    for (const payload of embeddedPayloads) {
        const extracted = extractPostFromUnknown(payload.data, {
            shortcodeHint: options.shortcodeHint,
            source: `${options.source}-script`,
            includeVisibleComments: options.includeVisibleComments,
            maxVisibleComments: options.maxVisibleComments,
        });

        if (!extracted) {
            continue;
        }

        extracted.post.sourcePath = payload.sourcePath + (extracted.post.sourcePath ? `.${extracted.post.sourcePath}` : '');
        candidates.push(extracted.post);
    }

    const metaExtracted = extractPostFromMetaSnapshot(snapshot, options);
    if (metaExtracted) {
        candidates.push(metaExtracted);
    }

    if (candidates.length === 0) {
        return null;
    }

    let mergedPost = candidates[0];
    for (const candidate of candidates.slice(1)) {
        if (candidate.shortcode !== mergedPost.shortcode) {
            continue;
        }

        mergedPost = mergeDownloadedPosts(mergedPost, candidate);
    }

    return mergedPost;
}

export function extractProfileFromHtml(
    html: string,
    options: {
        usernameHint?: string;
        includeRecentPosts?: boolean;
        maxRecentPosts?: number;
        source: string;
    },
): ExtractedProfile | null {
    const profileFromRegex = extractProfileFieldsFromHtml(html, options.usernameHint);
    if (!profileFromRegex) {
        return null;
    }

    const profile: ExtractedProfile = {
        ...profileFromRegex,
        source: options.source,
        score: 45,
    };

    if (options.includeRecentPosts) {
        const recentPosts = extractRecentPostsFromHtml(html, options.maxRecentPosts ?? 12, options.usernameHint);
        if (recentPosts.length > 0) {
            profile.recentPosts = recentPosts;
        }
    }

    return profile;
}

function findBestProfileCandidate(root: unknown, usernameHint?: string): { rawObject: JsonObject; path?: string; score: number } | null {
    const visited = new WeakSet<object>();
    const stack: Array<{ value: unknown; path: string[] }> = [{ value: root, path: [] }];

    let best: { rawObject: JsonObject; path?: string; score: number } | null = null;
    let exploredNodes = 0;

    while (stack.length > 0 && exploredNodes < 25_000) {
        const current = stack.pop()!;
        exploredNodes += 1;

        if (!isJsonObject(current.value)) {
            if (Array.isArray(current.value)) {
                for (let index = current.value.length - 1; index >= 0; index -= 1) {
                    stack.push({ value: current.value[index], path: current.path.concat(String(index)) });
                }
            }

            continue;
        }

        if (visited.has(current.value)) {
            continue;
        }

        visited.add(current.value);

        const score = scoreRawProfileCandidate(current.value, current.path, usernameHint);
        if (!best || score > best.score) {
            if (score > 0) {
                best = {
                    rawObject: current.value,
                    path: current.path.join('.'),
                    score,
                };
            }
        }

        for (const [key, value] of Object.entries(current.value)) {
            if (value === undefined || value === null) {
                continue;
            }

            stack.push({ value, path: current.path.concat(key) });
        }
    }

    return best?.score && best.score >= 35 ? best : null;
}

function scoreRawProfileCandidate(candidate: JsonObject, path: string[], usernameHint?: string): number {
    const username = takeString([
        candidate.username,
        getNested(candidate, ['user', 'username']),
        getNested(candidate, ['owner', 'username']),
    ])?.toLowerCase();

    if (!username || !USERNAME_PATTERN.test(username)) {
        return 0;
    }

    const fullName = takeString([
        candidate.full_name,
        candidate.name,
        getNested(candidate, ['user', 'full_name']),
    ]);

    const biography = takeString([
        candidate.biography,
        getNested(candidate, ['biography_with_entities', 'raw_text']),
    ]);

    const profilePicUrl = takeString([
        candidate.profile_pic_url_hd,
        candidate.profile_pic_url,
        getNested(candidate, ['hd_profile_pic_url_info', 'url']),
    ]);

    const followersCount = takeNumber([
        candidate.follower_count,
        candidate.followers_count,
        getNested(candidate, ['edge_followed_by', 'count']),
        getNested(candidate, ['followers', 'count']),
    ]);

    const followingCount = takeNumber([
        candidate.following_count,
        getNested(candidate, ['edge_follow', 'count']),
        getNested(candidate, ['following', 'count']),
    ]);

    const postsCount = takeNumber([
        candidate.media_count,
        candidate.posts_count,
        getNested(candidate, ['edge_owner_to_timeline_media', 'count']),
        getNested(candidate, ['timeline_media', 'count']),
    ]);

    let score = 20;

    if (usernameHint) {
        if (username === usernameHint) {
            score += 45;
        } else {
            score -= 30;
        }
    }

    if (fullName) score += 10;
    if (biography) score += 10;
    if (profilePicUrl) score += 10;
    if (followersCount !== undefined) score += 12;
    if (followingCount !== undefined) score += 8;
    if (postsCount !== undefined) score += 8;

    const joinedPath = path.join('.').toLowerCase();
    if (joinedPath.includes('web_profile_info')) score += 20;
    if (joinedPath.includes('graphql')) score += 10;
    if (joinedPath.endsWith('user') || joinedPath.includes('.user.')) score += 5;
    if (joinedPath.includes('owner')) score -= 10;
    if (joinedPath.includes('comment')) score -= 25;
    if (joinedPath.includes('liker')) score -= 25;
    if (joinedPath.includes('friendship')) score -= 10;

    return score;
}

function normalizeProfile(
    candidate: JsonObject,
    options: {
        source: string;
        sourcePath?: string;
        usernameHint?: string;
        score: number;
    },
): ExtractedProfile | null {
    const username = takeString([
        candidate.username,
        getNested(candidate, ['user', 'username']),
        getNested(candidate, ['owner', 'username']),
    ])?.toLowerCase();

    if (!username || !USERNAME_PATTERN.test(username)) {
        return null;
    }

    if (options.usernameHint && username !== options.usernameHint && options.score < 70) {
        return null;
    }

    return {
        username,
        fullName: takeString([
            candidate.full_name,
            candidate.name,
            getNested(candidate, ['user', 'full_name']),
        ]),
        biography: takeString([
            candidate.biography,
            getNested(candidate, ['biography_with_entities', 'raw_text']),
        ]),
        externalUrl: takeString([
            candidate.external_url,
            getNested(candidate, ['bio_links', '0', 'url']),
            getNested(candidate, ['bio_links', 0, 'url']),
        ]),
        profilePicUrl: takeString([
            candidate.profile_pic_url_hd,
            candidate.profile_pic_url,
            getNested(candidate, ['hd_profile_pic_url_info', 'url']),
        ]),
        isPrivate: takeBoolean([
            candidate.is_private,
            candidate.private,
        ]),
        isVerified: takeBoolean([
            candidate.is_verified,
            candidate.verified,
        ]),
        followersCount: takeNumber([
            candidate.follower_count,
            candidate.followers_count,
            getNested(candidate, ['edge_followed_by', 'count']),
            getNested(candidate, ['followers', 'count']),
        ]),
        followingCount: takeNumber([
            candidate.following_count,
            getNested(candidate, ['edge_follow', 'count']),
            getNested(candidate, ['following', 'count']),
        ]),
        postsCount: takeNumber([
            candidate.media_count,
            candidate.posts_count,
            getNested(candidate, ['edge_owner_to_timeline_media', 'count']),
            getNested(candidate, ['timeline_media', 'count']),
        ]),
        categoryName: takeString([
            candidate.category,
            candidate.category_name,
        ]),
        source: options.source,
        sourcePath: options.sourcePath,
        score: options.score,
    };
}

function findBestPostCandidate(root: unknown, shortcodeHint?: string): { rawObject: JsonObject; path?: string; score: number } | null {
    const visited = new WeakSet<object>();
    const stack: Array<{ value: unknown; path: string[] }> = [{ value: root, path: [] }];

    let best: { rawObject: JsonObject; path?: string; score: number } | null = null;
    let exploredNodes = 0;

    while (stack.length > 0 && exploredNodes < 35_000) {
        const current = stack.pop()!;
        exploredNodes += 1;

        if (!isJsonObject(current.value)) {
            if (Array.isArray(current.value)) {
                for (let index = current.value.length - 1; index >= 0; index -= 1) {
                    stack.push({ value: current.value[index], path: current.path.concat(String(index)) });
                }
            }

            continue;
        }

        if (visited.has(current.value)) {
            continue;
        }

        visited.add(current.value);

        const score = scoreRawPostCandidate(current.value, current.path, shortcodeHint);
        if (!best || score > best.score) {
            if (score > 0) {
                best = {
                    rawObject: current.value,
                    path: current.path.join('.'),
                    score,
                };
            }
        }

        for (const [key, value] of Object.entries(current.value)) {
            if (value === undefined || value === null) {
                continue;
            }

            stack.push({ value, path: current.path.concat(key) });
        }
    }

    return best?.score && best.score >= 35 ? best : null;
}

function scoreRawPostCandidate(candidate: JsonObject, path: string[], shortcodeHint?: string): number {
    const shortcode = extractPostShortcode(candidate);
    const displayUrl = extractPostDisplayUrl(candidate);
    const videoUrl = takeString([candidate.video_url, getNested(candidate, ['video_versions', 0, 'url'])]);
    const ownerUsername = extractPostOwnerUsername(candidate);
    const caption = extractPostCaptionText(candidate);
    const commentsCount = extractPostCommentsCount(candidate);
    const likesCount = extractPostLikesCount(candidate);
    const takenAtTimestamp = takeNumber([
        candidate.taken_at_timestamp,
        candidate.taken_at,
        candidate.created_at,
    ]);

    if (!shortcode && !displayUrl && !videoUrl) {
        return 0;
    }

    let score = 20;

    if (shortcodeHint) {
        if (shortcode && shortcode === shortcodeHint) {
            score += 50;
        } else {
            score -= 25;
        }
    }

    if (displayUrl) score += 15;
    if (videoUrl) score += 10;
    if (ownerUsername) score += 12;
    if (caption) score += 8;
    if (commentsCount !== undefined) score += 8;
    if (likesCount !== undefined) score += 8;
    if (takenAtTimestamp !== undefined) score += 6;

    const joinedPath = path.join('.').toLowerCase();
    if (joinedPath.includes('shortcode_media')) score += 30;
    if (joinedPath.includes('xdt_shortcode_media')) score += 25;
    if (joinedPath.includes('media')) score += 15;
    if (joinedPath.includes('graphql')) score += 10;
    if (joinedPath.includes('items.0')) score += 12;
    if (joinedPath.includes('comment')) score -= 18;
    if (joinedPath.includes('caption')) score -= 10;
    if (joinedPath.includes('owner')) score -= 5;

    return score;
}

function normalizePost(
    candidate: JsonObject,
    options: {
        source: string;
        sourcePath?: string;
        shortcodeHint?: string;
        score: number;
    },
): ExtractedPost | null {
    const shortcode = extractPostShortcode(candidate) ?? options.shortcodeHint;
    if (!shortcode || !SHORTCODE_PATTERN.test(shortcode)) {
        return null;
    }

    if (options.shortcodeHint && shortcode !== options.shortcodeHint && options.score < 75) {
        return null;
    }

    const mediaPath = inferPostMediaPath(candidate);
    const ownerUsername = extractPostOwnerUsername(candidate);
    const caption = extractPostCaptionText(candidate);
    const mediaType = takeNumber([candidate.media_type]);
    const typename = takeString([candidate.__typename, candidate.product_type])?.toLowerCase();
    const explicitIsVideo = takeBoolean([candidate.is_video]);
    const isVideo = explicitIsVideo
        ?? (mediaType === 2 ? true : undefined)
        ?? (typename?.includes('video') ? true : undefined);

    return {
        shortcode,
        url: mediaPath ? `https://www.instagram.com/${mediaPath}/${shortcode}/` : `https://www.instagram.com/p/${shortcode}/`,
        mediaPath,
        ownerUsername,
        ownerFullName: takeString([
            candidate.full_name,
            candidate.owner_full_name,
            candidate.name,
            getNested(candidate, ['owner', 'full_name']),
            getNested(candidate, ['user', 'full_name']),
        ]),
        caption,
        likesCount: extractPostLikesCount(candidate),
        commentsCount: extractPostCommentsCount(candidate),
        viewsCount: takeNumber([
            candidate.view_count,
            getNested(candidate, ['video_view_count']),
            getNested(candidate, ['play_count']),
        ]),
        playCount: takeNumber([
            candidate.play_count,
            getNested(candidate, ['video_play_count']),
        ]),
        isVideo,
        mediaType: takeString([
            candidate.product_type,
            candidate.__typename,
            typeof candidate.media_type === 'number' ? String(candidate.media_type) : undefined,
        ]),
        displayUrl: extractPostDisplayUrl(candidate),
        thumbnailUrl: takeString([
            candidate.thumbnail_src,
            getNested(candidate, ['thumbnail_resources', 0, 'src']),
            extractPostDisplayUrl(candidate),
        ]),
        videoUrl: takeString([
            candidate.video_url,
            getNested(candidate, ['video_versions', 0, 'url']),
        ]),
        takenAtTimestamp: normalizeTimestamp(takeNumber([
            candidate.taken_at_timestamp,
            candidate.taken_at,
            candidate.created_at,
        ])),
        locationName: takeString([
            getNested(candidate, ['location', 'name']),
            getNested(candidate, ['location', 'short_name']),
        ]),
        source: options.source,
        sourcePath: options.sourcePath,
        score: options.score,
    };
}

function extractVisibleCommentsFromUnknown(
    root: unknown,
    maxVisibleComments?: number,
    options?: {
        ownerUsername?: string;
        caption?: string;
    },
): PostComment[] {
    const visited = new WeakSet<object>();
    const stack: Array<{ value: unknown; path: string[] }> = [{ value: root, path: [] }];
    const comments: PostComment[] = [];
    const seen = new Set<string>();
    const normalizedCaption = normalizeWhitespace(options?.caption);
    const limit = maxVisibleComments ?? Number.MAX_SAFE_INTEGER;
    let exploredNodes = 0;

    while (stack.length > 0 && comments.length < limit && exploredNodes < 50_000) {
        const current = stack.pop()!;
        exploredNodes += 1;

        if (!isJsonObject(current.value)) {
            if (Array.isArray(current.value)) {
                for (let index = current.value.length - 1; index >= 0; index -= 1) {
                    stack.push({ value: current.value[index], path: current.path.concat(String(index)) });
                }
            }

            continue;
        }

        if (visited.has(current.value)) {
            continue;
        }

        visited.add(current.value);

        const score = scoreRawCommentCandidate(current.value, current.path);
        if (score >= 25) {
            const comment = normalizePostComment(current.value, current.path);
            if (comment) {
                if (
                    normalizedCaption
                    && options?.ownerUsername
                    && comment.username === options.ownerUsername
                    && normalizeWhitespace(comment.text) === normalizedCaption
                ) {
                    continue;
                }

                const key = comment.id
                    ?? `${comment.username ?? 'unknown'}:${normalizeWhitespace(comment.text)}:${comment.takenAtTimestamp ?? ''}`;

                if (!seen.has(key)) {
                    seen.add(key);
                    comments.push(comment);
                }
            }
        }

        for (const [key, value] of Object.entries(current.value)) {
            if (value === undefined || value === null) {
                continue;
            }

            stack.push({ value, path: current.path.concat(key) });
        }
    }

    return comments.slice(0, limit);
}

function scoreRawCommentCandidate(candidate: JsonObject, path: string[]): number {
    const text = extractCommentText(candidate);
    if (!text) {
        return 0;
    }

    let score = 10;

    if (extractCommentUsername(candidate)) score += 12;
    if (takeString([candidate.id, candidate.pk, candidate.comment_id])) score += 8;
    if (takeNumber([candidate.created_at, candidate.created_at_utc, candidate.created_at_ts])) score += 5;
    if (takeNumber([candidate.comment_like_count, candidate.like_count])) score += 4;

    const joinedPath = path.join('.').toLowerCase();
    if (joinedPath.includes('comment')) score += 20;
    if (joinedPath.includes('reply')) score += 10;
    if (joinedPath.includes('preview')) score += 5;
    if (joinedPath.includes('caption')) score -= 25;
    if (joinedPath.includes('media')) score -= 5;

    if (extractPostShortcode(candidate) || extractPostDisplayUrl(candidate) || takeString([candidate.profile_pic_url, candidate.profile_pic_url_hd])) {
        score -= 20;
    }

    return score;
}

function normalizePostComment(candidate: JsonObject, path: string[]): PostComment | null {
    const text = normalizeWhitespace(extractCommentText(candidate));
    if (!text) {
        return null;
    }

    const username = extractCommentUsername(candidate);
    const timestamp = normalizeTimestamp(takeNumber([
        candidate.created_at,
        candidate.created_at_utc,
        candidate.created_at_ts,
        candidate.taken_at,
    ]));

    return {
        id: takeString([
            candidate.id,
            candidate.pk,
            candidate.comment_id,
            getNested(candidate, ['node', 'id']),
        ]),
        username,
        fullName: takeString([
            getNested(candidate, ['user', 'full_name']),
            getNested(candidate, ['owner', 'full_name']),
        ]),
        text,
        likeCount: takeNumber([
            candidate.comment_like_count,
            candidate.like_count,
            getNested(candidate, ['edge_liked_by', 'count']),
        ]),
        takenAtTimestamp: timestamp,
        profileUrl: username ? `https://www.instagram.com/${username}/` : undefined,
        isReply: path.join('.').toLowerCase().includes('reply') || candidate.parent_comment_id !== undefined,
    };
}

function extractRecentPosts(root: unknown, maxRecentPosts: number, usernameHint?: string): RecentPost[] {
    const visited = new WeakSet<object>();
    const stack: unknown[] = [root];
    const posts: RecentPost[] = [];
    const seen = new Set<string>();

    while (stack.length > 0 && posts.length < maxRecentPosts) {
        const current = stack.pop();
        if (!current) {
            continue;
        }

        if (Array.isArray(current)) {
            for (let index = current.length - 1; index >= 0; index -= 1) {
                stack.push(current[index]);
            }

            continue;
        }

        if (!isJsonObject(current)) {
            continue;
        }

        if (visited.has(current)) {
            continue;
        }

        visited.add(current);

        const normalizedPost = normalizeRecentPost(current, usernameHint);
        if (normalizedPost) {
            const key = normalizedPost.shortcode ?? normalizedPost.id ?? normalizedPost.url ?? normalizedPost.displayUrl;
            if (key && !seen.has(key)) {
                seen.add(key);
                posts.push(normalizedPost);
            }
        }

        for (const value of Object.values(current)) {
            if (value !== undefined && value !== null) {
                stack.push(value);
            }
        }
    }

    return posts.slice(0, maxRecentPosts);
}

function normalizeRecentPost(candidate: JsonObject, usernameHint?: string): RecentPost | null {
    const ownerUsername = takeString([
        getNested(candidate, ['owner', 'username']),
        getNested(candidate, ['user', 'username']),
    ])?.toLowerCase();

    if (usernameHint && ownerUsername && ownerUsername !== usernameHint) {
        return null;
    }

    const shortcode = takeString([
        candidate.shortcode,
        candidate.code,
    ]);

    const displayUrl = takeString([
        candidate.display_url,
        candidate.thumbnail_src,
        getNested(candidate, ['image_versions2', 'candidates', 0, 'url']),
    ]);

    const videoUrl = takeString([
        candidate.video_url,
    ]);

    const id = takeString([
        candidate.id,
        candidate.pk,
    ]);

    if (!shortcode && !displayUrl && !id) {
        return null;
    }

    const caption = takeString([
        getNested(candidate, ['edge_media_to_caption', 'edges', 0, 'node', 'text']),
        getNested(candidate, ['caption', 'text']),
        candidate.caption,
        candidate.accessibility_caption,
    ]);

    const likesCount = takeNumber([
        getNested(candidate, ['edge_media_preview_like', 'count']),
        getNested(candidate, ['edge_liked_by', 'count']),
        candidate.like_count,
    ]);

    const commentsCount = takeNumber([
        getNested(candidate, ['edge_media_to_comment', 'count']),
        getNested(candidate, ['edge_media_to_parent_comment', 'count']),
        candidate.comment_count,
    ]);

    const takenAtTimestamp = takeNumber([
        candidate.taken_at_timestamp,
        candidate.taken_at,
    ]);

    const url = shortcode ? `https://www.instagram.com/p/${shortcode}/` : undefined;

    return {
        id,
        shortcode,
        url,
        caption,
        commentsCount,
        likesCount,
        isVideo: takeBoolean([candidate.is_video]),
        displayUrl,
        thumbnailUrl: takeString([candidate.thumbnail_src, displayUrl]),
        videoUrl,
        takenAtTimestamp,
    };
}

function extractPostShortcode(candidate: JsonObject): string | undefined {
    const shortcode = takeString([
        candidate.shortcode,
        candidate.code,
        getNested(candidate, ['node', 'shortcode']),
        getNested(candidate, ['media', 'shortcode']),
        getNested(candidate, ['shortcode_media', 'shortcode']),
    ]);

    return shortcode && SHORTCODE_PATTERN.test(shortcode) ? shortcode : undefined;
}

function inferPostMediaPath(candidate: JsonObject): PostPathSegment | undefined {
    const productType = takeString([candidate.product_type])?.toLowerCase();
    if (productType === 'clips') {
        return 'reel';
    }

    if (productType === 'igtv') {
        return 'tv';
    }

    return undefined;
}

function extractPostOwnerUsername(candidate: JsonObject): string | undefined {
    const username = takeString([
        candidate.username,
        candidate.owner_username,
        getNested(candidate, ['owner', 'username']),
        getNested(candidate, ['user', 'username']),
        getNested(candidate, ['caption', 'user', 'username']),
    ])?.toLowerCase();

    return username && USERNAME_PATTERN.test(username) ? username : undefined;
}

function extractPostCaptionText(candidate: JsonObject): string | undefined {
    const caption = takeString([
        getNested(candidate, ['edge_media_to_caption', 'edges', 0, 'node', 'text']),
        getNested(candidate, ['caption', 'text']),
        typeof candidate.caption === 'string' ? candidate.caption : undefined,
        getNested(candidate, ['node', 'text']),
    ]);

    return normalizeWhitespace(caption);
}

function extractPostDisplayUrl(candidate: JsonObject): string | undefined {
    return takeString([
        candidate.display_url,
        candidate.display_src,
        candidate.thumbnail_src,
        getNested(candidate, ['image_versions2', 'candidates', 0, 'url']),
        getNested(candidate, ['carousel_media', 0, 'image_versions2', 'candidates', 0, 'url']),
    ]);
}

function extractPostCommentsCount(candidate: JsonObject): number | undefined {
    return takeNumber([
        candidate.comment_count,
        candidate.comments_count,
        candidate.preview_comment_count,
        getNested(candidate, ['edge_media_to_parent_comment', 'count']),
        getNested(candidate, ['edge_media_to_comment', 'count']),
    ]);
}

function extractPostLikesCount(candidate: JsonObject): number | undefined {
    return takeNumber([
        candidate.like_count,
        candidate.likes_count,
        getNested(candidate, ['edge_media_preview_like', 'count']),
        getNested(candidate, ['edge_liked_by', 'count']),
    ]);
}

function extractCommentText(candidate: JsonObject): string | undefined {
    return takeString([
        candidate.text,
        getNested(candidate, ['node', 'text']),
        getNested(candidate, ['content_text']),
        getNested(candidate, ['caption', 'text']),
    ]);
}

function extractCommentUsername(candidate: JsonObject): string | undefined {
    const username = takeString([
        getNested(candidate, ['user', 'username']),
        getNested(candidate, ['owner', 'username']),
        getNested(candidate, ['node', 'owner', 'username']),
    ])?.toLowerCase();

    return username && USERNAME_PATTERN.test(username) ? username : undefined;
}

function extractEmbeddedJsonPayloads(
    html: string,
    jsonLd: string[],
): Array<{ data: unknown; sourcePath: string }> {
    const payloads: Array<{ data: unknown; sourcePath: string }> = [];
    const seenBodies = new Set<string>();

    const scriptMatches = html.matchAll(/<script\b[^>]*type=(?:"|')(application\/json|application\/ld\+json)(?:"|')[^>]*>([\s\S]*?)<\/script>/gi);
    let index = 0;
    for (const match of scriptMatches) {
        const body = match[2]?.trim();
        if (!body || body.length > 2_000_000 || seenBodies.has(body)) {
            index += 1;
            continue;
        }

        seenBodies.add(body);
        const parsed = safeJsonParse(body);
        if (parsed !== null) {
            payloads.push({
                data: parsed,
                sourcePath: `html-script-${index}`,
            });
        }

        index += 1;
    }

    for (const [jsonLdIndex, body] of jsonLd.entries()) {
        const trimmed = body.trim();
        if (!trimmed || trimmed.length > 500_000 || seenBodies.has(trimmed)) {
            continue;
        }

        seenBodies.add(trimmed);
        const parsed = safeJsonParse(trimmed);
        if (parsed !== null) {
            payloads.push({
                data: parsed,
                sourcePath: `jsonld-${jsonLdIndex}`,
            });
        }
    }

    return payloads;
}

function extractPostFromMetaSnapshot(
    snapshot: DomSnapshot,
    options: {
        shortcodeHint?: string;
        mediaPathHint?: PostPathSegment;
        source: string;
    },
): ExtractedPost | null {
    const decodedDescription = decodeHtmlEntities(
        takeString([
            snapshot.meta['og:description'],
            snapshot.meta.description,
            snapshot.meta['twitter:description'],
        ]),
    );
    const decodedTitle = decodeHtmlEntities(
        takeString([
            snapshot.meta['og:title'],
            snapshot.meta.title,
            snapshot.title,
        ]),
    );
    const displayUrl = decodeHtmlEntities(
        takeString([
            snapshot.meta['og:image'],
            snapshot.meta['twitter:image'],
        ]),
    );
    const videoUrl = decodeHtmlEntities(
        takeString([
            snapshot.meta['og:video'],
            snapshot.meta['og:video:secure_url'],
            snapshot.meta['twitter:player:stream'],
        ]),
    );

    const descriptionText = decodedDescription ?? '';
    const titleText = decodedTitle ?? '';
    const shortcode = options.shortcodeHint ?? extractShortcodeFromPostUrl(snapshot.url);
    const ownerUsername = extractOwnerUsernameFromPostMeta(descriptionText);
    const ownerFullName = extractOwnerFullNameFromPostMeta(titleText);
    const caption = extractCaptionFromPostMeta(descriptionText, titleText);
    const likesCount = extractCountFromLabel(descriptionText, 'likes?');
    const commentsCount = extractCountFromLabel(descriptionText, 'comments?');
    const takenAtTimestamp = extractTimestampFromPostMeta(descriptionText);
    const mediaPath = options.mediaPathHint ?? extractMediaPathFromPostUrl(snapshot.url);
    const isVideo = videoUrl ? true : undefined;

    if (
        !shortcode
        && !ownerUsername
        && !ownerFullName
        && !caption
        && likesCount === undefined
        && commentsCount === undefined
        && !displayUrl
        && !videoUrl
    ) {
        return null;
    }

    return {
        shortcode: shortcode ?? snapshot.url,
        url: shortcode ? `https://www.instagram.com/${mediaPath ?? 'p'}/${shortcode}/` : snapshot.url,
        mediaPath,
        ownerUsername,
        ownerFullName,
        caption,
        likesCount,
        commentsCount,
        isVideo,
        mediaType: isVideo ? 'video' : (displayUrl ? 'image' : undefined),
        displayUrl,
        thumbnailUrl: displayUrl,
        videoUrl,
        takenAtTimestamp,
        source: `${options.source}-meta`,
        score: 60
            + (caption ? 12 : 0)
            + (likesCount !== undefined ? 8 : 0)
            + (commentsCount !== undefined ? 8 : 0)
            + (displayUrl ? 6 : 0),
    };
}

function mergeDownloadedPosts(base: ExtractedPost, candidate: ExtractedPost): ExtractedPost {
    const primary = downloadedPostRichnessScore(base) >= downloadedPostRichnessScore(candidate) ? base : candidate;
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
        visibleComments: mergeDownloadedComments(primary.visibleComments, secondary.visibleComments),
        source: primary.source,
        sourcePath: primary.sourcePath ?? secondary.sourcePath,
        score: Math.max(primary.score, secondary.score),
    };
}

function downloadedPostRichnessScore(post: ExtractedPost): number {
    let score = 0;
    if (post.ownerUsername) score += 2;
    if (post.ownerFullName) score += 1;
    if (post.caption) score += 3;
    if (post.likesCount !== undefined) score += 2;
    if (post.commentsCount !== undefined) score += 2;
    if (post.displayUrl) score += 2;
    if (post.videoUrl) score += 2;
    if (post.takenAtTimestamp !== undefined) score += 1;
    if (post.visibleComments?.length) score += Math.min(post.visibleComments.length, 5);
    return score;
}

function mergeDownloadedComments(primary: PostComment[] | undefined, secondary: PostComment[] | undefined): PostComment[] | undefined {
    const merged = new Map<string, PostComment>();

    for (const comment of [...(primary ?? []), ...(secondary ?? [])]) {
        const key = comment.id ?? `${comment.username ?? 'unknown'}:${comment.text}:${comment.takenAtTimestamp ?? ''}`;
        const existing = merged.get(key);
        if (!existing) {
            merged.set(key, comment);
            continue;
        }

        merged.set(key, {
            id: existing.id ?? comment.id,
            username: existing.username ?? comment.username,
            fullName: existing.fullName ?? comment.fullName,
            text: existing.text || comment.text,
            likeCount: existing.likeCount ?? comment.likeCount,
            takenAtTimestamp: existing.takenAtTimestamp ?? comment.takenAtTimestamp,
            profileUrl: existing.profileUrl ?? comment.profileUrl,
            isReply: existing.isReply ?? comment.isReply,
        });
    }

    const values = Array.from(merged.values());
    return values.length > 0 ? values : undefined;
}

function extractProfileFieldsFromHtml(html: string, usernameHint?: string): Omit<ExtractedProfile, 'source' | 'score'> | null {
    const username = usernameHint ?? extractEscapedString(html, 'username');
    if (!username) {
        return null;
    }

    const followersCount = extractNumericField(html, ['follower_count', 'followers_count']);
    const followingCount = extractNumericField(html, ['following_count']);
    const postsCount = extractNumericField(html, ['media_count', 'posts_count']);

    return {
        username,
        fullName: extractEscapedString(html, 'full_name') ?? undefined,
        biography: extractEscapedString(html, 'biography') ?? undefined,
        externalUrl: extractEscapedString(html, 'external_url') ?? undefined,
        profilePicUrl: extractEscapedString(html, 'profile_pic_url_hd')
            ?? extractEscapedString(html, 'profile_pic_url')
            ?? undefined,
        isPrivate: extractBooleanField(html, 'is_private'),
        isVerified: extractBooleanField(html, 'is_verified'),
        followersCount,
        followingCount,
        postsCount,
        categoryName: extractEscapedString(html, 'category_name') ?? undefined,
    };
}

function extractRecentPostsFromHtml(html: string, maxRecentPosts: number, usernameHint?: string): RecentPost[] {
    const shortcodeMatches = [...html.matchAll(/"shortcode":"((?:[^"\\]|\\.)+)"/g)];
    if (shortcodeMatches.length === 0) {
        return [];
    }

    const posts: RecentPost[] = [];
    const seen = new Set<string>();

    for (const match of shortcodeMatches) {
        const shortcode = decodeEscapedString(match[1]);
        if (!shortcode || seen.has(shortcode)) {
            continue;
        }

        seen.add(shortcode);
        const boundaryStart = Math.max(0, match.index - 800);
        const boundaryEnd = Math.min(html.length, (match.index ?? 0) + 4000);
        const snippet = html.slice(boundaryStart, boundaryEnd);

        const ownerUsername = extractEscapedString(snippet, 'username');
        if (usernameHint && ownerUsername && ownerUsername.toLowerCase() !== usernameHint) {
            continue;
        }

        posts.push({
            shortcode,
            url: `https://www.instagram.com/p/${shortcode}/`,
            id: extractEscapedString(snippet, 'id') ?? undefined,
            caption: extractEscapedString(snippet, 'text') ?? undefined,
            commentsCount: extractNumericField(snippet, ['comment_count']),
            likesCount: extractNumericField(snippet, ['like_count']),
            isVideo: extractBooleanField(snippet, 'is_video'),
            displayUrl: extractEscapedString(snippet, 'display_url') ?? extractEscapedString(snippet, 'thumbnail_src') ?? undefined,
            thumbnailUrl: extractEscapedString(snippet, 'thumbnail_src') ?? undefined,
            videoUrl: extractEscapedString(snippet, 'video_url') ?? undefined,
            takenAtTimestamp: extractNumericField(snippet, ['taken_at_timestamp', 'taken_at']),
        });

        if (posts.length >= maxRecentPosts) {
            break;
        }
    }

    return posts;
}

function extractCountsFromText(text: string): {
    postsCount?: number;
    followersCount?: number;
    followingCount?: number;
} {
    const normalized = text.replace(/\u00a0/g, ' ');
    const postsCount = extractCountFromLabel(normalized, 'posts?');
    const followersCount = extractCountFromLabel(normalized, 'followers?');
    const followingCount = extractCountFromLabel(normalized, 'following');

    return { postsCount, followersCount, followingCount };
}

function extractShortcodeFromPostUrl(url: string): string | undefined {
    try {
        const parsed = new URL(url);
        const segments = parsed.pathname.split('/').filter(Boolean);
        const mediaPath = segments[0]?.toLowerCase();
        const shortcode = segments[1];
        if (!mediaPath || !POST_PATH_SEGMENTS.has(mediaPath as PostPathSegment)) {
            return undefined;
        }

        return shortcode && SHORTCODE_PATTERN.test(shortcode) ? shortcode : undefined;
    } catch {
        return undefined;
    }
}

function extractMediaPathFromPostUrl(url: string): PostPathSegment | undefined {
    try {
        const parsed = new URL(url);
        const segment = parsed.pathname.split('/').filter(Boolean)[0]?.toLowerCase();
        return segment && POST_PATH_SEGMENTS.has(segment as PostPathSegment) ? segment as PostPathSegment : undefined;
    } catch {
        return undefined;
    }
}

function extractCountFromLabel(text: string, labelPattern: string): number | undefined {
    const regex = new RegExp(`([0-9.,]+(?:\\s*[KMB])?)\\s+${labelPattern}`, 'i');
    const match = text.match(regex);
    if (!match) {
        return undefined;
    }

    return parseFlexibleNumber(match[1]);
}

function extractFullName(text: string, username: string): string | undefined {
    const normalized = text.trim();
    if (!normalized) {
        return undefined;
    }

    const cleaned = normalized
        .replace(/\s*•\s*Instagram.*$/i, '')
        .replace(/\s*on Instagram.*$/i, '')
        .replace(new RegExp(`\\((?:@)?${escapeRegExp(username)}\\)`, 'i'), '')
        .replace(/\((?:@)?[a-z0-9._]{1,30}\)/i, '')
        .trim();

    if (!cleaned) {
        return undefined;
    }

    return cleaned;
}

function extractBiography(text: string, username: string, fullName?: string): string | undefined {
    const parts = text
        .split('\n')
        .map((part) => part.trim())
        .filter(Boolean);

    for (const part of parts) {
        const lowered = part.toLowerCase();
        if (lowered.includes('followers') || lowered.includes('following') || lowered.includes('posts')) {
            continue;
        }

        if (part.includes(`@${username}`) || part.toLowerCase() === username.toLowerCase()) {
            continue;
        }

        if (fullName && part === fullName) {
            continue;
        }

        if (part.length >= 10) {
            return part;
        }
    }

    return undefined;
}

function extractOwnerUsernameFromPostMeta(text: string): string | undefined {
    const match = text.match(/-\s*([a-zA-Z0-9._]{1,30})\s+on\s+/i);
    return match?.[1]?.toLowerCase();
}

function extractOwnerFullNameFromPostMeta(text: string): string | undefined {
    const normalized = text.trim();
    if (!normalized) {
        return undefined;
    }

    const match = normalized.match(/^(.*?)\s+on Instagram\s*:/i);
    const candidate = match?.[1]?.trim();
    return candidate || undefined;
}

function extractCaptionFromPostMeta(description: string, title: string): string | undefined {
    const descriptionMatch = description.match(/:\s*(.+)$/s);
    const titleMatch = title.match(/on Instagram\s*:\s*(.+)$/is);
    const candidate = descriptionMatch?.[1] ?? titleMatch?.[1];
    if (!candidate) {
        return undefined;
    }

    return stripWrappingQuotes(candidate)
        ?.replace(/\s+\.$/, '')
        .trim() || undefined;
}

function extractTimestampFromPostMeta(text: string): number | undefined {
    const match = text.match(/\son\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})(?::|\s)/);
    if (!match?.[1]) {
        return undefined;
    }

    const timestamp = Date.parse(match[1]);
    return Number.isFinite(timestamp) ? Math.floor(timestamp / 1_000) : undefined;
}

function extractUsernameFromUrl(url: string): string | undefined {
    try {
        const parsed = new URL(url);
        const segment = parsed.pathname.split('/').filter(Boolean)[0];
        if (segment && USERNAME_PATTERN.test(segment) && !RESERVED_PATH_SEGMENTS.has(segment.toLowerCase())) {
            return segment.toLowerCase();
        }

        return undefined;
    } catch {
        return undefined;
    }
}

function extractUsernameFromText(text: string): string | undefined {
    const match = text.match(/@([a-zA-Z0-9._]{1,30})/);
    return match?.[1]?.toLowerCase();
}

function extractEscapedString(text: string, fieldName: string): string | undefined {
    const regex = new RegExp(`"${escapeRegExp(fieldName)}":"((?:[^"\\\\]|\\\\.)*)"`);
    const match = text.match(regex);
    return match ? decodeEscapedString(match[1]) : undefined;
}

function extractBooleanField(text: string, fieldName: string): boolean | undefined {
    const regex = new RegExp(`"${escapeRegExp(fieldName)}":(true|false)`);
    const match = text.match(regex);
    if (!match) {
        return undefined;
    }

    return match[1] === 'true';
}

function extractNumericField(text: string, fieldNames: string[]): number | undefined {
    for (const fieldName of fieldNames) {
        const regex = new RegExp(`"${escapeRegExp(fieldName)}":([0-9]+(?:\\.[0-9]+)?)`);
        const match = text.match(regex);
        if (match) {
            return parseFlexibleNumber(match[1]);
        }
    }

    return undefined;
}

function decodeEscapedString(value: string): string {
    try {
        return JSON.parse(`"${value.replace(/"/g, '\\"')}"`);
    } catch {
        return value;
    }
}

function decodeHtmlEntities(value: string | undefined): string | undefined {
    if (!value) {
        return undefined;
    }

    return value.replace(/&(#x?[0-9a-f]+|quot|amp|apos|lt|gt);/gi, (match, entity: string) => {
        const normalized = entity.toLowerCase();
        if (normalized === 'quot') return '"';
        if (normalized === 'amp') return '&';
        if (normalized === 'apos') return '\'';
        if (normalized === 'lt') return '<';
        if (normalized === 'gt') return '>';
        if (normalized.startsWith('#x')) {
            const codePoint = Number.parseInt(normalized.slice(2), 16);
            return Number.isFinite(codePoint) ? String.fromCodePoint(codePoint) : match;
        }
        if (normalized.startsWith('#')) {
            const codePoint = Number.parseInt(normalized.slice(1), 10);
            return Number.isFinite(codePoint) ? String.fromCodePoint(codePoint) : match;
        }

        return match;
    });
}

function stripWrappingQuotes(value: string | undefined): string | undefined {
    if (!value) {
        return undefined;
    }

    const trimmed = value.trim();
    const unwrapped = trimmed
        .replace(/^["'“”‘’]+/, '')
        .replace(/["'“”‘’]+\s*\.?$/, '')
        .trim();

    return unwrapped || undefined;
}

function takeString(values: unknown[]): string | undefined {
    for (const value of values) {
        if (typeof value === 'string') {
            const trimmed = value.trim();
            if (trimmed) {
                return trimmed;
            }
        }
    }

    return undefined;
}

function takeBoolean(values: unknown[]): boolean | undefined {
    for (const value of values) {
        if (typeof value === 'boolean') {
            return value;
        }
    }

    return undefined;
}

function takeNumber(values: unknown[]): number | undefined {
    for (const value of values) {
        const parsed = parseUnknownNumber(value);
        if (parsed !== undefined) {
            return parsed;
        }
    }

    return undefined;
}

function parseUnknownNumber(value: unknown): number | undefined {
    if (typeof value === 'number' && Number.isFinite(value)) {
        return value;
    }

    if (typeof value === 'string') {
        return parseFlexibleNumber(value);
    }

    return undefined;
}

function parseFlexibleNumber(value: string): number | undefined {
    const normalized = value.trim().replace(/,/g, '');
    if (!normalized) {
        return undefined;
    }

    const suffix = normalized.slice(-1).toUpperCase();
    const multiplier = suffix === 'K' ? 1_000 : suffix === 'M' ? 1_000_000 : suffix === 'B' ? 1_000_000_000 : 1;
    const numericPart = multiplier === 1 ? normalized : normalized.slice(0, -1);
    const parsed = Number(numericPart);

    if (!Number.isFinite(parsed)) {
        return undefined;
    }

    return Math.round(parsed * multiplier);
}

function normalizeTimestamp(value: number | undefined): number | undefined {
    if (value === undefined) {
        return undefined;
    }

    if (value > 1_000_000_000_000) {
        return Math.floor(value / 1_000);
    }

    return value;
}

function normalizeWhitespace(value: string | undefined): string | undefined {
    if (!value) {
        return undefined;
    }

    const normalized = value.replace(/\u00a0/g, ' ').replace(/\s+/g, ' ').trim();
    return normalized || undefined;
}

function getNested(value: unknown, path: Array<string | number>): unknown {
    let current = value;

    for (const key of path) {
        if (Array.isArray(current) && typeof key === 'number') {
            current = current[key];
            continue;
        }

        if (isJsonObject(current) && typeof key === 'string') {
            current = current[key];
            continue;
        }

        return undefined;
    }

    return current;
}

function isJsonObject(value: unknown): value is JsonObject {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function escapeRegExp(value: string): string {
    return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}
