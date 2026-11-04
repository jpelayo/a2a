/**
 * a2a — the DeepSeek Harness (dsh) entry: a Cordis plugin.
 *
 * NO default export, on purpose. dsh's loader resolves a plugin as
 * `module.default ?? module` (vendor/loader), so a default here would hide
 * `name`, `inject` and `apply`. The client itself is a2a-dsh.js — dsh's own,
 * served with the broker url and token baked in — and `apply` only hands
 * the context to it.
 *
 * Installed by the a2a installer into ~/.dsh/a2a/plugin and added to a
 * profile with dsh's own command:
 *
 *     dsh plugin --profile web add link:$HOME/.dsh/a2a/plugin
 */
import a2a from "./a2a-dsh.js"

export const name = "a2a"
// Required services: without one the plugin stays pending and apply never
// runs. The session controller and profile context are read with ctx.get.
export const inject = ["agents", "tools", "systemPrompt"]

export async function apply(ctx) {
  await a2a.dsh(ctx)
}
