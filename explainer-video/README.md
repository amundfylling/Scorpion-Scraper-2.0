# Scorpion Scraper explainer video

A short [Remotion](https://www.remotion.dev/) video (about 2 minutes, 1920×1080)
that explains, in plain language, how the scraper collects data each night:
tournaments → matches → players → saved to GitHub.

The scenes live in `src/scenes.tsx`; scene lengths are set in `DURATIONS`.

```bash
cd explainer-video
npm install
npm run studio   # preview and scrub through the video in the browser
npm run render   # writes out/scorpion-scraper-explained.mp4
```

The statistics on the "Everything is saved" scene are the row counts of the
files in `data/` from October 2026; update `stats` in `src/scenes.tsx` to
refresh them. Player and tournament names in the video are made up.
