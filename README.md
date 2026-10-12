# Mexico TV schedules

Run `python scraper.py`. It installs missing `requests`, `beautifulsoup4`, and
`Pillow` packages using the current interpreter. Both schedule sources live in
this script. GitHub Actions runs it before `image_downloader.py`.

## Channel selection

Use one `mi.tv-slug=XMLTV-ID` pair per line in `filter.txt`:

```text
fx=fx
a-e-mundo=A&E
hbo-plus-mexico=HBOPlusMéxico
```

The left side remains the mi.tv URL slug, JSON filename and
key in `all-channel.json`. The right side matches the XMLTV channel ID without
`.mx`. Matching allows differences in capitalization but preserves punctuation
and accents; `A+` and `A` are different channels. Write `A&E` normally in this
file: the XML parser decodes `A&amp;E` automatically. Blank lines and `#` comments
are allowed. Invalid or duplicate entries are reported as configuration errors.

## Source priority and times

The script downloads https://iptv-epg.org/files/epg-mx.xml once per run and uses
IPTV first for every configured channel. If downloading or parsing fails, the
channel is absent, or it has no usable programmes starting today in UTC-6, it
tries mi.tv with the left-hand slug. Schedules are selected per channel, not
mixed across sources. The existing mi.tv timing and formatting logic is retained.

Plain XML, gzip file bodies, and HTTP gzip compression are supported. Override
the feed with `XMLTV_URL`, including a `.xml.gz` URL if needed.

IPTV timestamps are converted from their supplied offset (usually UTC) to fixed
UTC-6 before grouping by local date. The output window remains today, tomorrow,
and yesterday's programmes ending after 23:00. Explicit stop times are preserved;
missing stops use the next programme or one hour for the last programme. Invalid
programme timestamps are skipped and counted. Separate episodes are not merged.

## JSON fields

The existing `channel` and date-to-programme-list structure remains intact.
For IPTV, `channel` contains the display name with the `MX -`/`MX-` prefix
removed and XML entities decoded, for example `A&E`. There is no separate
`channel_name` field. Filenames and image directories still use the left-hand slug.

Programme fields remain `show`, `category`, `logo`, `start`, and `end`, in that
exact order, followed by the new metadata fields.
IPTV programmes additionally have:

- `release_year`: first four characters of XMLTV `date`, as a string.
- `description`: Spanish description when available.
- `episode_number`: the XMLTV episode value, such as `S5 E4`.

Missing metadata is an empty string. Spanish titles and categories are preferred;
when unavailable, the available language is used. Multiple categories are joined
with `/`, for example `Comedia/Romance/Película`. The `logo` uses the programme
icon. Today's combined JSON retains these programme fields and the existing CDN
image rewriting. mi.tv output keeps its previous fields.

Run tests with `python -m unittest discover -s tests -v`.

## Image reuse and GitHub Actions

`image_downloader.py` processes each channel file separately. It groups identical
image URLs first, then identical show names (case and whitespace normalized).
Each group uses one image. All original programme fields and their order are
preserved. JSON filenames and image folders use the first filter name, never
the display name stored in `channel`.

The SQLite database at `image-state/images.sqlite3` remembers URL and show-name
aliases per channel across runs. Only successful images are indexed. Existing
local WebP images from earlier versions are reused where their filenames match.
A database entry alone is insufficient: the file must exist and pass image
validation. Missing or corrupt files are downloaded again; failed downloads use
the existing fallback image and can be retried when fresh schedules arrive.
Images retain the existing 300×300 maximum size, WebP quality 30, and parallel
downloads. Today's `all-channel.json` is rebuilt after processing so its URLs
agree with individual schedules, including fallback URLs and duplicate images.

The workflow no longer commits or pushes to GitHub. It retains the existing VPS
archive upload and extraction, using the existing `VPS_PASSWORD`, `VPS_USERNAME`,
and `VPS_IP` GitHub secrets. No R2 configuration is used.

Before scraping, the workflow restores images and the SQLite database from the
VPS with rsync over SSH. After processing, it uploads and extracts the images,
schedules, and finally the database. SQLite is closed before archiving. Only one
workflow run can update this shared state at a time. No GitHub cache or committed
database is required.

VPS paths:

- Schedules: `/data/programaciontv/schedule/`
- Images: `/data/programaciontv/wp-content/uploads/downloaded-images/`
- Database: `/data/programaciontv/image-state/images.sqlite3`

The first run creates the state directory and initializes a database if none is
present. Existing VPS images are restored and reused. An SSH or restore failure
stops the workflow instead of proceeding with missing state. The VPS must have
`rsync` installed; the runner installs its transfer tools automatically. Existing
VPS files are not deleted during synchronization.

Fresh schedule output goes to `run-schedule/` in Actions so old repository JSONs
are not accidentally republished. `SCHEDULE_DIR`, `IMAGE_DB_PATH`, and
`IMAGE_CDN_BASE` can also be overridden for local runs. The default CDN URL remains
unchanged. The database and temporary run output are ignored by Git.
