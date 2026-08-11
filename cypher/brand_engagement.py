"""Writes ENGAGED_WITH, POSTED_IN and REACTED_IN, plus the nodes they connect.

All three are singleton aggregate edges — one per (engager, brand) or (account,
channel), recomputed and overwritten each run — because a per-reaction edge on a
popular brand account would swamp every other signal in the graph.

The two node-materialising statements exist so every participant and channel
seen in the event-level CSVs is present even when its summary row was filtered
out upstream by a score threshold.
"""

from __future__ import annotations

PARTICIPANTS_CYPHER = """
UNWIND $rows AS row
MERGE (a:WarpcastAccount {fid: row.fid})
  ON CREATE SET a.asOf = $asOf, a.ingestedBy = $ingestedBy
"""

ENGAGEMENT_SUMMARY_CYPHER = """
UNWIND $rows AS row
MERGE (e:WarpcastAccount {fid: row.engager_fid})
  ON CREATE SET e.asOf = $asOf, e.ingestedBy = $ingestedBy
MERGE (b:WarpcastAccount {fid: row.brand_fid})
  ON CREATE SET b.asOf = $asOf, b.ingestedBy = $ingestedBy, b.isBrand = true
MERGE (e)-[r:ENGAGED_WITH]->(b)
SET r.replies = row.replies,
    r.likes = row.likes,
    r.recasts = row.recasts,
    r.mentions = row.mentions,
    r.weightedScore = row.weighted_score,
    r.windowStart = row.window_start,
    r.windowEnd = row.window_end,
    r.asOf = $asOf,
    r.ingestedBy = $ingestedBy
"""

CHANNEL_NODES_CYPHER = """
UNWIND $rows AS row
MERGE (c:Channel {channelId: row.channel_id})
SET c.asOf = $asOf, c.ingestedBy = $ingestedBy
FOREACH (_ IN CASE WHEN row.fid IS NULL THEN [] ELSE [1] END |
  MERGE (a:WarpcastAccount {fid: row.fid})
    ON CREATE SET a.asOf = $asOf, a.ingestedBy = $ingestedBy
)
"""

CHANNEL_SUMMARY_CYPHER = """
UNWIND $rows AS row
MERGE (a:WarpcastAccount {fid: row.fid})
  ON CREATE SET a.asOf = $asOf, a.ingestedBy = $ingestedBy
MERGE (c:Channel {channelId: row.channel_id})
  ON CREATE SET c.asOf = $asOf, c.ingestedBy = $ingestedBy
MERGE (a)-[p:POSTED_IN]->(c)
SET p.castCount = row.casts_posted,
    p.firstAt = row.first_cast_at,
    p.lastAt = row.last_cast_at,
    p.asOf = $asOf,
    p.ingestedBy = $ingestedBy
MERGE (a)-[r:REACTED_IN]->(c)
SET r.given = row.reactions_given,
    r.received = row.reactions_received,
    r.repliesReceived = row.replies_received,
    r.asOf = $asOf,
    r.ingestedBy = $ingestedBy
"""
