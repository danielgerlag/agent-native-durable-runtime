import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import Ajv2020 from "ajv/dist/2020";
import { Checkpoint, load } from "../src/index";

function repoRoot(): string {
  let dir = __dirname;
  for (;;) {
    if (fs.existsSync(path.join(dir, "fixtures", "v0", "tools.view.json"))) {
      return dir;
    }
    const parent = path.dirname(dir);
    if (parent === dir) {
      throw new Error(`fixtures/v0 not found from ${__dirname}`);
    }
    dir = parent;
  }
}

function readJson(filePath: string): unknown {
  const raw: unknown = JSON.parse(fs.readFileSync(filePath, "utf8"));
  return raw;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

const root = repoRoot();
const fixtures = path.join(root, "fixtures", "v0");
const schemaDir = path.join(root, "spec", "schema");

test("tools view matches golden", () => {
  const cp = load(path.join(fixtures, "tools"));
  const golden = readJson(path.join(fixtures, "tools.view.json"));
  assert.deepEqual(cp.view(), golden);
  assert.equal(cp.sessionId, "sess_demo");
  assert.equal(cp.events.length, 7);
  assert.equal(cp.messages.length, 4);
  assert.deepEqual(Checkpoint.load(path.join(fixtures, "tools")).view(), golden);
});

test("canonical view and golden files validate against spec/schema", () => {
  const ajv = new Ajv2020({ allErrors: true, allowUnionTypes: true });
  const viewSchema = readJson(path.join(schemaDir, "view.schema.json"));
  const eventSchema = readJson(path.join(schemaDir, "event.schema.json"));
  const manifestSchema = readJson(path.join(schemaDir, "manifest.schema.json"));
  if (!isRecord(viewSchema) || !isRecord(eventSchema) || !isRecord(manifestSchema)) {
    throw new Error("schema is not an object");
  }
  const validateView = ajv.compile(viewSchema);
  const validateEvent = ajv.compile(eventSchema);
  const validateManifest = ajv.compile(manifestSchema);

  const cp = load(path.join(fixtures, "tools"));
  const golden = readJson(path.join(fixtures, "tools.view.json"));
  assert.equal(validateView(cp.view()), true, ajv.errorsText(validateView.errors));
  assert.equal(validateView(golden), true, ajv.errorsText(validateView.errors));

  const manifest = readJson(path.join(fixtures, "tools", "manifest.json"));
  assert.equal(
    validateManifest(manifest),
    true,
    ajv.errorsText(validateManifest.errors),
  );

  const transcript = fs.readFileSync(
    path.join(fixtures, "tools", "transcript.ndjson"),
    "utf8",
  );
  for (const line of transcript.split(/\r?\n/)) {
    if (line.trim() === "") {
      continue;
    }
    const event: unknown = JSON.parse(line);
    assert.equal(
      validateEvent(event),
      true,
      `${ajv.errorsText(validateEvent.errors)} in ${line}`,
    );
  }
});

test("seq gap is rejected", () => {
  assert.throws(
    () => load(path.join(fixtures, "invalid-seq-gap")),
    (err: unknown) => err instanceof Error && err.message.includes("seq gap"),
  );
});

test("unsupported checkpoint_version is rejected", () => {
  assert.throws(
    () => load(path.join(fixtures, "invalid-version")),
    (err: unknown) =>
      err instanceof Error && err.message.includes("checkpoint_version"),
  );
});
