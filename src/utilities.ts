import {
  GraffitiErrorInvalidSchema,
  GraffitiErrorInvalidUri,
  GraffitiErrorPatchError,
  GraffitiErrorPatchTestFailed,
} from "@graffiti-garden/api";
import type {
  Graffiti,
  GraffitiObjectBase,
  GraffitiLocation,
  GraffitiPatch,
  JSONSchema4,
  GraffitiSession,
} from "@graffiti-garden/api";
import type { Ajv } from "ajv";
import type { applyPatch } from "fast-json-patch";

export const locationToUri: Graffiti["locationToUri"] = (location) => {
  return `${location.source}/${encodeURIComponent(location.actor)}/${encodeURIComponent(location.name)}`;
};

export const uriToLocation: Graffiti["uriToLocation"] = (uri) => {
  const parts = uri.split("/");
  const nameEncoded = parts.pop();
  const webIdEncoded = parts.pop();
  if (!nameEncoded || !webIdEncoded || !parts.length) {
    throw new GraffitiErrorInvalidUri();
  }
  return {
    name: decodeURIComponent(nameEncoded),
    actor: decodeURIComponent(webIdEncoded),
    source: parts.join("/"),
  };
};

export function randomBase64(numBytes: number = 16) {
  const bytes = new Uint8Array(numBytes);
  crypto.getRandomValues(bytes);
  // Convert it to base64
  const base64 = btoa(String.fromCodePoint(...bytes));
  // Make sure it is url safe
  return base64.replace(/\+/g, "-").replace(/\//g, "_").replace(/\=+$/, "");
}

export function unpackLocationOrUri(locationOrUri: GraffitiLocation | string) {
  if (typeof locationOrUri === "string") {
    return {
      location: uriToLocation(locationOrUri),
      uri: locationOrUri,
    };
  } else {
    return {
      location: {
        name: locationOrUri.name,
        actor: locationOrUri.actor,
        source: locationOrUri.source,
      },
      uri: locationToUri(locationOrUri),
    };
  }
}

export function applyGraffitiPatch<Prop extends keyof GraffitiPatch>(
  apply: typeof applyPatch,
  prop: Prop,
  patch: GraffitiPatch,
  object: GraffitiObjectBase,
): void {
  const ops = patch[prop];
  if (!ops || !ops.length) return;
  try {
    object[prop] = apply(object[prop], ops, true, false).newDocument;
  } catch (e) {
    if (
      typeof e === "object" &&
      e &&
      "name" in e &&
      typeof e.name === "string" &&
      "message" in e &&
      typeof e.message === "string"
    ) {
      if (e.name === "TEST_OPERATION_FAILED") {
        throw new GraffitiErrorPatchTestFailed(e.message);
      } else {
        throw new GraffitiErrorPatchError(e.name + ": " + e.message);
      }
    } else {
      throw e;
    }
  }
}

export function attemptAjvCompile<Schema extends JSONSchema4>(
  ajv: Ajv,
  schema: Schema,
) {
  try {
    return ajv.compile(schema);
  } catch (error) {
    throw new GraffitiErrorInvalidSchema(
      error instanceof Error ? error.message : undefined,
    );
  }
}

export function maskGraffitiObject(
  object: GraffitiObjectBase,
  channels: string[],
  session?: GraffitiSession | null,
): void {
  if (object.actor !== session?.actor) {
    object.allowed = object.allowed && session ? [session.actor] : undefined;
    object.channels = object.channels.filter((channel) =>
      channels.includes(channel),
    );
  }
}
export function isActorAllowedGraffitiObject(
  object: GraffitiObjectBase,
  session?: GraffitiSession | null,
) {
  return (
    object.allowed === undefined ||
    (!!session?.actor &&
      (object.actor === session.actor ||
        object.allowed.includes(session.actor)))
  );
}
