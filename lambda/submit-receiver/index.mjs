// ═══════════════════════════════════════════════════
// Lambda A — submit-receiver
// Receives a candidate's submission (from submission.js via
// API Gateway), tries a direct DynamoDB write first. Only on
// throttling does it fall back to SQS (the overflow buffer).
// Always returns 200 quickly — the frontend never waits on
// whether the write went direct or via SQS.
// ═══════════════════════════════════════════════════

import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand } from "@aws-sdk/lib-dynamodb";
import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";

const ddbClient = new DynamoDBClient({});
const ddb = DynamoDBDocumentClient.from(ddbClient);
const sqs = new SQSClient({});

const TABLE_NAME = "submissions";
const QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/414061810318/submissions-queue";

function slugify(str) {
  return (str || "unknown")
    .toString()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/(^-|-$)/g, "");
}

function buildItem(payload) {
  const info = payload.candidateInfo || {};
  // IMPORTANT: prefer the stable, curated examId (set by the frontend's
  // exam dropdown) over the raw scraped "exam" text from the source page.
  // The scraped text can vary slightly between candidates/shifts (spacing,
  // capitalization, "CBT I" vs "CBT-1" vs "CBT1"), which would fragment
  // one exam's candidates across multiple different PK groups and break
  // rank calculation. Only fall back to the scraped text if examId is
  // genuinely missing.
  //
  // ALSO IMPORTANT: examId must be cycle-unique on its own (e.g.
  // "rrb-ntpc-ug-cbt1-cen0102024"), not just exam-type-unique
  // (e.g. "rrb-ntpc-ug-cbt1"). This is intentionally NOT derived
  // automatically from the candidate's shift date here — a single
  // recruitment cycle can span a year boundary (shifts in Dec and the
  // following Jan), and calendar-year-from-date would incorrectly split
  // one real cycle into two groups. The exam dropdown in calculator.html
  // is where this must be encoded, since that's the one place with
  // actual knowledge of the exam's official notification/CEN cycle —
  // append the cycle identifier to examId there when adding a new exam
  // option, rather than relying on any individual candidate's date.
  const examSlug = payload.examId ? slugify(payload.examId) : slugify(info.exam);
  const date = info.date || "unknown-date";
  const shift = slugify(info.shift || "unknown-shift");
  const rollNo = info.rollNo || "unknown-roll";

  const totalScore = Number(payload.totals?.totalScore || 0);
  // Zero-padded, fixed-width so DynamoDB's natural string sort on
  // GSI2SK gives correct highest-to-lowest ordering when queried
  // with ScanIndexForward: false. 4 digits before decimal covers
  // scores up to 9999 — plenty of headroom for any exam's max marks.
  const paddedScore = totalScore >= 0
    ? totalScore.toFixed(2).padStart(7, "0")
    : "0000000"; // negative net scores still sort last; adjust if needed

  return {
    PK: `EXAM#${examSlug}#${date}#${shift}`,
    SK: `ROLL#${rollNo}`,
    GSI1PK: `EXAM#${examSlug}`,
    GSI1SK: `SHIFT#${date}#${shift}#ROLL#${rollNo}`,
    GSI2PK: `EXAM#${examSlug}#${date}#${shift}`,
    GSI2SK: `SCORE#${paddedScore}`,
    sourceUrl: payload.sourceUrl || null,
    family: payload.family || null,
    examId: payload.examId || null,
    candidateInfo: info,
    formFields: payload.formFields || {},
    sections: payload.sections || [],
    totals: payload.totals || {},
    marking: payload.marking || {},
    submittedAt: payload.submittedAt || new Date().toISOString(),
    receivedAt: new Date().toISOString() // server-side timestamp, authoritative
  };
}

export const handler = async (event) => {
  let payload;
  try {
    payload = typeof event.body === "string" ? JSON.parse(event.body) : event.body;
  } catch (err) {
    return {
      statusCode: 400,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ error: "Invalid JSON body" })
    };
  }

  if (!payload || !payload.candidateInfo || !payload.candidateInfo.rollNo) {
    return {
      statusCode: 400,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ error: "Missing required candidateInfo.rollNo" })
    };
  }

  const item = buildItem(payload);

  try {
    // ── Try the direct write first — this is the fast path that
    // handles the overwhelming majority of requests, since DynamoDB
    // On-demand comfortably absorbs normal traffic.
    await ddb.send(new PutCommand({ TableName: TABLE_NAME, Item: item }));

    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ status: "ok", via: "direct" })
    };
  } catch (err) {
    const isThrottle =
      err.name === "ProvisionedThroughputExceededException" ||
      err.name === "ThrottlingException" ||
      err.name === "RequestLimitExceeded";

    if (!isThrottle) {
      // A real, unexpected error (bad IAM perms, malformed item, etc.)
      // — surface it rather than silently masking it as a queue fallback.
      console.error("Unexpected DynamoDB error:", err);
      return {
        statusCode: 500,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ error: "Internal error" })
      };
    }

    // ── DynamoDB is genuinely under pressure (spike beyond what
    // On-demand auto-scaling has warmed up for). Fall back to SQS
    // so the submission is never lost — Lambda B will retry the
    // write once the burst settles.
    try {
      await sqs.send(new SendMessageCommand({
        QueueUrl: QUEUE_URL,
        MessageBody: JSON.stringify(item)
      }));

      return {
        statusCode: 200,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ status: "ok", via: "queued" })
      };
    } catch (sqsErr) {
      // Both direct write AND queueing failed — genuinely rare, but
      // must not pretend success to the frontend in this case.
      console.error("SQS fallback also failed:", sqsErr);
      return {
        statusCode: 503,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ error: "Service temporarily unavailable" })
      };
    }
  }
};
