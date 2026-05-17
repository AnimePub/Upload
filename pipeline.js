// pipeline.js — core anime upload logic

const SKIP_GENRES = ["Josei", "Yuri"];

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function makeFinder(name) {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .trim()
    .replace(/\s+/g, "-");
}

function ratingFromScore(score) {
  const s = parseFloat(score) || 0;
  if (s <= 2) return 10;
  if (s <= 4) return 20;
  if (s <= 6) return 30;
  if (s <= 8) return 40;
  return 50;
}

async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status} → ${url}`);
  return res.json();
}

async function fetchText(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status} → ${url}`);
  return (await res.text()).replace(/"/g, "").trim();
}

async function postJSON(url, body, cookie = null) {
  const headers = { "Content-Type": "application/json" };
  if (cookie) headers["Cookie"] = `anipub=${cookie}`;
  const res = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} → ${url}`);
  return res.json();
}

async function syncEpisodes(anipubId, episodes, malid, cookie, log) {
  if (episodes.length === 0) {
    log("    No episodes found.");
    return;
  }

  let currentCount = 0;
  try {
    const found = await fetchJSON(`https://www.anipub.xyz/api/info/${anipubId}`);
    currentCount = Number(found?.epCount+1) || 0 +1 ;
    log(`    Current ep count on anipub: ${currentCount+1}`);
  } catch (e) {
    log(`    Could not fetch epCount, assuming 0: ${e.message}`, "warn");
  }

  const APIArray = [];
  for (const ep of episodes) {
    const epNum = parseInt(ep.num) || 0;
    if (epNum <= currentCount) continue;
    APIArray.push({
      link: `src=https://anipub.xyz/play/${malid}/${epNum}/sub`,
    });
  }

  if (APIArray.length === 0) {
    log("    All episodes already synced.");
    return;
  }

  log(`    Bulk adding ${APIArray.length} missing episode(s)...`);
  const resp = await postJSON(
    "https://anipub.xyz/Bulk/Add",
    { ID: anipubId, ARY: APIArray },
    cookie
  );

  if (Number(resp) === 1) {
    log("    ✓ Bulk add successful.", "ok");
  } else {
    throw new Error("Bulk/Add returned: " + JSON.stringify(resp));
  }
}

async function processAnime(item, index, total, cookie, log) {
  const slug = item.url?.split("/watch/")?.[1];
  log(`\n[${index}/${total}] ${item.title}`);

  if (!slug) {
    log("  ✗ Could not extract slug — skipping.", "error");
    return "skipped";
  }

  const info = await fetchJSON(`https://anikoto-api.onrender.com/info?name=${slug}`);
  const genres = info.genres || [];

  const skipReason = genres.find((g) => SKIP_GENRES.includes(g));
  if (skipReason) {
    log(`  ↷ Skipped (genre: ${skipReason})`, "skip");
    return "skipped";
  }

  const check = await postJSON("https://anipub.xyz/api/check", {
    Name: info.title,
    Genre: genres,
  });

  if (check.exists) {
    log("  Already exists — checking for missing episodes...", "info");

    // Get page ID and episodes to sync missing ones
    const pageId = await fetchText(`https://anikoto-api.onrender.com/page?name=${slug}`);
    const episodes = await fetchJSON(`https://anikoto-api.onrender.com/episodes?id=${pageId}`);
    const malid = episodes[0]?.malid || "";

    // Find the anipub ID by finder
    const finder = makeFinder(info.title);
    let anipubId = null;
    try {
	
      const found = await fetchJSON(`https://www.anipub.xyz/api/find/${encodeURIComponent(info.title)}`);
      anipubId = found?.id || found?.ID || null;
    } catch (e) {
      log(`  Could not find anipub ID by finder: ${e.message}`, "warn");
    }

    if (anipubId) {
      await syncEpisodes(anipubId, episodes, malid, cookie, log);
    } else {
      log("  Could not resolve anipub ID — episode sync skipped.", "warn");
    }

    return "synced";
  }

  // New anime — full upload flow
  const pageId = await fetchText(`https://anikoto-api.onrender.com/page?name=${slug}`);
  log(`  Page ID: ${pageId}`);

  const episodes = await fetchJSON(`https://anikoto-api.onrender.com/episodes?id=${pageId}`);
  log(`  Episodes found: ${episodes.length}`);

  const malid = episodes[0]?.malid || "";

  const lastNum = parseInt(await fetchText("https://anipub.xyz/api/getLast"));
  const newNum = lastNum + 1;
  log(`  New anipub ID: ${newNum}`);

  const isFinished = (info.status || "").toLowerCase().includes("finished");
  const score = parseFloat(info.rating) || 0;
  const finder = makeFinder(info.title);
  log(`  Finder: ${finder}`);

  const uploadInfo = {
    id: newNum,
    epName: episodes[0]?.title || "Episode 1",
    Name: info.title,
    finder: finder,
    ip: info.poster,
    cover: info.poster,
    syn: "",
    link: `src=https://anipub.xyz/play/${malid}/1/sub`,
    title: info.title,
    aired: info.aired || "",
    premiered: info.premiered || "",
    duration: info.duration || "",
    Status: isFinished ? "Finished" : "Ongoing",
    malscore: info.rating || "0",
    ratings: ratingFromScore(score),
    studios: (info.studios || []).join(", "),
    producers: (info.producers || []).join(", "),
    MALID: malid,
    genre: genres,
    des: info.synopsis || "",
    type: "iframe",
  };

  const uploadResp = await postJSON("https://anipub.xyz/upload", uploadInfo, cookie);

  if (Number(uploadResp) !== 1) {
    throw new Error("Upload returned: " + JSON.stringify(uploadResp));
  }
  log("  ✓ Main entry uploaded.", "ok");

  await syncEpisodes(newNum, episodes, malid, cookie, log);

  log(`  ✓ Done: ${info.title}`, "ok");
  return "added";
}

async function runPipeline({ date, cookie, delayMs = 3000, onLog, onProgress, onDone }) {
  const log = (msg, type = "info") => onLog?.({ msg, type, time: new Date().toISOString() });

  log(`=== Pipeline started | Date: ${date} ===`, "info");

  let schedule;
  try {
    schedule = await fetchJSON(`https://anikoto-api.onrender.com/schedule?time=${date}`);
    log(`Schedule has ${schedule.length} anime.`, "info");
  } catch (e) {
    log(`Failed to fetch schedule: ${e.message}`, "error");
    onDone?.({ added: 0, skipped: 0, synced: 0, errors: 1 });
    return;
  }

  let added = 0, skipped = 0, synced = 0, errors = 0;

  for (let i = 0; i < schedule.length; i++) {
    onProgress?.({ current: i + 1, total: schedule.length });
    try {
      const result = await processAnime(schedule[i], i + 1, schedule.length, cookie, log);
      if (result === "added") added++;
      else if (result === "synced") synced++;
      else skipped++;
    } catch (e) {
      log(`  ✗ Error: ${e.message}`, "error");
      errors++;
    }

    if (i < schedule.length - 1) {
      log(`  Waiting ${delayMs / 1000}s...`, "info");
      await sleep(delayMs);
    }
  }

  const summary = { added, skipped, synced, errors };
  log(`=== Finished | Added:${added} Synced:${synced} Skipped:${skipped} Errors:${errors} ===`, "ok");
  onDone?.(summary);
  return summary;
}

module.exports = { runPipeline };
