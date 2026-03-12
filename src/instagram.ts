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

export interface NormalizedProfileTarget {
    originalUrl: string;
    normalizedUrl: string;
    usernameHint: string;
}

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

interface SearchResult {
    profile: ExtractedProfile;
    rawObject: JsonObject;
}

const scrapeInstagramMetadata = metascraper([
    metascraperInstagram(),
    metascraperTitle(),
    metascraperDescription({ truncateLength: Number.MAX_SAFE_INTEGER }),
    metascraperImage(),
    metascraperUrl(),
]);

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
        originalUrl: rawUrl,
        normalizedUrl,
        usernameHint: username.toLowerCase(),
    };
}

export function dedupeTargets(targets: NormalizedProfileTarget[]): NormalizedProfileTarget[] {
    const seen = new Set<string>();
    const uniqueTargets: NormalizedProfileTarget[] = [];

    for (const target of targets) {
        if (seen.has(target.normalizedUrl)) {
            continue;
        }

        seen.add(target.normalizedUrl);
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
): SearchResult | null {
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
