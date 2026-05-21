const $ = (selector) => document.querySelector(selector);

const configStatus = $("#configStatus");
const exportButton = $("#exportButton");
const databaseButton = $("#databaseButton");
const publishAllButton = $("#publishAllButton");
const scheduleAllButton = $("#scheduleAllButton");
const createButton = $("#createButton");
const postsButton = $("#postsButton");

loadConfig();

exportButton.addEventListener("click", async () => {
  await runButton(exportButton, async () => {
    const result = await api("/api/media/export", {
      method: "POST",
      body: {
        force: $("#forceDownload").checked
      }
    });
    $("#exportResult").textContent = JSON.stringify(result, null, 2);
    if (result.downloadUrl) window.location.href = result.downloadUrl;
  });
});

databaseButton.addEventListener("click", async () => {
  await runButton(databaseButton, async () => {
    const result = await api("/api/database/import", {
      method: "POST",
      body: {}
    });
    $("#databaseResult").textContent = JSON.stringify(result, null, 2);
  });
});

publishAllButton.addEventListener("click", async () => {
  await runButton(publishAllButton, async () => {
    const result = await api("/api/buffer/publish-all", {
      method: "POST",
      body: batchPayload()
    });
    $("#batchResult").textContent = JSON.stringify(result, null, 2);
  });
});

scheduleAllButton.addEventListener("click", async () => {
  await runButton(scheduleAllButton, async () => {
    const result = await api("/api/buffer/schedule-all", {
      method: "POST",
      body: {
        ...batchPayload(),
        postsPerDay: 3,
        startDate: $("#scheduleStartDate").value || undefined
      }
    });
    $("#batchResult").textContent = JSON.stringify(result, null, 2);
  });
});

createButton.addEventListener("click", async () => {
  await runButton(createButton, async () => {
    const assetUrl = $("#assetUrl").value.trim();
    const localDueAt = $("#dueAt").value;
    const body = {
      text: $("#postText").value,
      channelId: $("#channelId").value.trim() || undefined,
      mode: $("#postMode").value,
      dueAt: localDueAt ? new Date(localDueAt).toISOString() : undefined,
      assets: assetUrl ? [{ type: $("#assetType").value, url: assetUrl }] : []
    };

    const result = await api("/api/buffer/posts", {
      method: "POST",
      body
    });
    $("#createResult").textContent = JSON.stringify(result, null, 2);
  });
});

postsButton.addEventListener("click", async () => {
  await runButton(postsButton, async () => {
    const params = new URLSearchParams({
      status: $("#statusFilter").value,
      first: $("#firstCount").value
    });
    const after = $("#afterCursor").value.trim();
    if (after) params.set("after", after);

    const result = await api(`/api/buffer/posts?${params.toString()}`);
    $("#postsResult").textContent = JSON.stringify(result, null, 2);
  });
});

async function loadConfig() {
  try {
    const config = await api("/api/config");
    $("#channelId").placeholder = config.channelId || "Usa BUFFER_CHANNEL_ID se vazio";
    $("#batchChannelId").placeholder = config.channelId || "Usa BUFFER_CHANNEL_ID se vazio";
    configStatus.textContent = [
      `Dataset: ${config.datasetFile}`,
      `Pasta: ${config.outputDir}`,
      `Postgres: ${config.hasDatabaseUrl ? "configurado" : "pendente"}`,
      `Buffer API Key: ${config.hasBufferApiKey ? "configurada" : "pendente"}`,
      `Channel ID: ${config.channelId || "pendente"}`,
      `Organization ID: ${config.organizationId || "pendente"}`,
      `Agendamento: ${config.bufferScheduleTimes.join(", ")}`
    ].join("\n");
  } catch (error) {
    configStatus.textContent = error.message;
  }
}

async function runButton(button, callback) {
  const originalText = button.textContent;
  button.disabled = true;
  button.textContent = "Processando...";
  try {
    await callback();
  } catch (error) {
    const target = button === exportButton
      ? "#exportResult"
      : button === databaseButton
        ? "#databaseResult"
        : button === publishAllButton || button === scheduleAllButton
          ? "#batchResult"
          : button === createButton
            ? "#createResult"
            : "#postsResult";
    $(target).textContent = error.message;
  } finally {
    button.disabled = false;
    button.textContent = originalText;
  }
}

function batchPayload() {
  const limit = $("#batchLimit").value;
  return {
    force: $("#forceBatch").checked,
    channelId: $("#batchChannelId").value.trim() || undefined,
    limit: limit ? Number(limit) : undefined
  };
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    method: options.method || "GET",
    headers: options.body ? { "Content-Type": "application/json" } : {},
    body: options.body ? JSON.stringify(options.body) : undefined
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(data.error || `HTTP ${response.status}`);
  }
  return data;
}
