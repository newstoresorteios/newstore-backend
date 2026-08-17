export const MANUAL_BATCH_SIZE = 50;
export const MANUAL_MAX_CAMPAIGN_USERS = 500;

export const EMAIL_ALL_CONSENTED_SUPPORTED = false;
export const EMAIL_ALL_CONSENTED_UNAVAILABLE_REASON = "email_consent_not_available";

export function estimatedManualBatches(uniqueUsers) {
  const count = Number(uniqueUsers);
  if (!Number.isFinite(count) || count <= 0) return 0;
  return Math.ceil(count / MANUAL_BATCH_SIZE);
}

export function chunkManualAudience(values, size = MANUAL_BATCH_SIZE) {
  const rows = Array.isArray(values) ? values : [];
  const chunks = [];
  for (let offset = 0; offset < rows.length; offset += size) {
    chunks.push(rows.slice(offset, offset + size));
  }
  return chunks;
}

export function assertManualCampaignAudienceSize(uniqueUsers) {
  const count = Number(uniqueUsers);
  if (Number.isInteger(count) && count <= MANUAL_MAX_CAMPAIGN_USERS) return;
  const error = new Error("manual_audience_too_large");
  error.code = "manual_audience_too_large";
  error.max = MANUAL_MAX_CAMPAIGN_USERS;
  throw error;
}
