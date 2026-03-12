# Instagram Profile and Post Scraper Actor

Apify Actor that scrapes Instagram profile data and Instagram post/reel data from input URL lists using `PlaywrightCrawler`.

It is wired for Apify platform features that matter on Instagram:

- Apify Proxy or custom proxies through `proxyConfiguration`
- Browser fingerprints through Crawlee's browser pool
- Session pool with cookie persistence and rotation
- Playwright browser automation with request retries
- Downloaded page parsing through `metascraper` + `metascraper-instagram`

## Input

See [`.actor/input_schema.json`](/work/.actor/input_schema.json) for the full input definition.

At minimum:

```json
{
  "profileUrls": [
    "https://www.instagram.com/instagram/",
    "https://www.instagram.com/natgeo/"
  ],
  "postUrls": [
    "https://www.instagram.com/p/SHORTCODE/",
    "https://www.instagram.com/reel/SHORTCODE/"
  ],
  "proxyConfiguration": {
    "useApifyProxy": true
  }
}
```

## Output

One dataset item per URL.

Profile items include:

- normalized profile URL
- username
- full name
- biography
- follower, following, and post counts
- profile picture URL
- privacy / verification flags
- recent profile posts when available
- scrape metadata such as proxy URL, session id, and extraction source

Post items include:

- normalized post URL and shortcode
- owner username / full name when available
- caption, media URLs, media type, and taken-at timestamp when available
- likes / comments / plays / views when available
- `visibleComments`, including comments loaded after clicking or scrolling the page
- scrape metadata such as proxy URL, session id, and extraction source

## Local Testing

Docker is the recommended local test path, because this Actor depends on Playwright browser libraries that may be missing on the host machine.

Create local actor input:

```bash
mkdir -p storage/key_value_stores/default
cat > storage/key_value_stores/default/INPUT.json <<'JSON'
{
  "profileUrls": [
    "https://www.instagram.com/instagram/"
  ],
  "postUrls": [
    "https://www.instagram.com/p/SHORTCODE/"
  ],
  "maxConcurrency": 1,
  "debugLogResponses": true,
  "includeRecentPosts": true,
  "maxRecentPosts": 5,
  "includeVisibleComments": true
}
JSON
```

Build and run with Docker:

```bash
docker build -t instagram-profile-scraper ./

mkdir -p storage/{datasets,key_value_stores/default,request_queues}
sudo chown -R "$(id -u):$(id -g)" storage

docker run --rm -it \
  --user "$(id -u):$(id -g)" \
  -e APIFY_PROXY_PASSWORD="${APIFY_PROXY_PASSWORD}" \
  -e CRAWLEE_STORAGE_DIR=/usr/src/app/storage \
  -e APIFY_LOCAL_STORAGE_DIR=/usr/src/app/storage \
  -v "$(pwd)/storage:/usr/src/app/storage" \
  instagram-profile-scraper
```

Results are written to:

- `storage/datasets/default/`
- `storage/key_value_stores/default/`

Quick inspection:

```bash
ls -la ./storage/datasets/default
jq . ./storage/datasets/default/*.json
```

If you want to run natively instead:

```bash
cd /work
export APIFY_LOCAL_STORAGE_DIR=./storage
npx playwright install chromium
npm start
```

If the native Playwright launch fails on missing shared libraries, install them on the host or use Docker instead. On Debian/Ubuntu:

```bash
sudo npx playwright install-deps chromium
```

## Notes

- Public Instagram structure changes often. This Actor first captures JSON responses from the browser session and then falls back to DOM / downloaded-page heuristics when needed.
- Post comment extraction is based on what is visibly rendered in the browser session. The actor clicks comment expansion controls and scrolls comment containers to pull in additional visible comments before extraction.
- For private profiles or stronger login walls, provide `initialCookies` from a logged-in Instagram session.
