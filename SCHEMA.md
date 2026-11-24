# herdr API Schema (protocol 22, schema v1)

Generated from `herdr api schema --json` (herdr 0.9.5). herdcluster speaks this NDJSON API: one JSON request per connection, response is a single JSON line.

## Envelope

Request: `{"id": "<string>", "method": "<method>", "params": {...}}` — `id` must be a JSON string.

Response: `{"id": ..., "result": {...}}` or `{"id": ..., "error": {"code", "message"}}`.

Total methods: **102**

## `agent.*`

| Method | Params | Required |
|---|---|---|
| `agent.explain` | `target`: string **req** | `target` |
| `agent.focus` | `target`: string **req** | `target` |
| `agent.get` | `target`: string **req** | `target` |
| `agent.list` | — | — |
| `agent.prompt` | `target`: string **req**<br>`text`: string **req**<br>`wait`: AgentPromptWaitOptions | `target`, `text` |
| `agent.read` | `format`: ReadFormat<br>`lines`: integer|null/uint32<br>`source`: ReadSource **req**<br>`strip_ansi`: boolean<br>`target`: string **req** | `target`, `source` |
| `agent.rename` | `name`: string|null<br>`target`: string **req** | `target` |
| `agent.send_keys` | `keys`: array **req**<br>`target`: string **req** | `target`, `keys` |
| `agent.start` | `args`: array<br>`kind`: string **req**<br>`name`: string **req**<br>`pane_id`: string **req**<br>`timeout_ms`: integer|null/uint64 | `name`, `kind`, `pane_id` |
| `agent.view.clear` | `source`: string|null | — |
| `agent.view.set` | `filter`: AgentViewFilter<br>`label`: string|null<br>`sort`: array<br>`source`: string **req** | `source` |
| `agent.wait` | `target`: string **req**<br>`timeout_ms`: integer|null/uint64<br>`until`: array | `target` |

## `client.*`

| Method | Params | Required |
|---|---|---|
| `client.window_title.clear` | — | — |
| `client.window_title.set` | `title`: string **req** | `title` |

## `client_shell.*`

| Method | Params | Required |
|---|---|---|
| `client_shell.surface.set` | `active`: boolean **req** | `active` |

## `command.*`

| Method | Params | Required |
|---|---|---|
| `command.invoke` | `command_id`: string **req**<br>`pane_id`: string|null<br>`selection`: PaneSelectionReadParams<br>`tab_id`: string|null<br>`workspace_id`: string|null | `command_id` |

## `events.*`

| Method | Params | Required |
|---|---|---|
| `events.subscribe` | `subscriptions`: array **req** | `subscriptions` |
| `events.wait` | `match_event`: EventMatch **req**<br>`timeout_ms`: integer|null/uint64 | `match_event` |

## `integration.*`

| Method | Params | Required |
|---|---|---|
| `integration.install` | `target`: IntegrationTarget **req** | `target` |
| `integration.list` | — | — |
| `integration.uninstall` | `target`: IntegrationTarget **req** | `target` |

## `layout.*`

| Method | Params | Required |
|---|---|---|
| `layout.apply` | `focus`: boolean<br>`root`: LayoutNode **req**<br>`tab_id`: string|null<br>`tab_label`: string|null<br>`workspace_id`: string|null | `root` |
| `layout.export` | `pane_id`: string|null<br>`tab_id`: string|null | — |
| `layout.set_split_ratio` | `pane_id`: string|null<br>`path`: array **req**<br>`ratio`: number/float **req**<br>`tab_id`: string|null | `path`, `ratio` |

## `notification.*`

| Method | Params | Required |
|---|---|---|
| `notification.show` | `body`: string|null<br>`position`: ToastHerdrPosition<br>`sound`: NotificationShowSound<br>`title`: string **req** | `title` |

## `pane.*`

| Method | Params | Required |
|---|---|---|
| `pane.clear` | `pane_id`: string **req** | `pane_id` |
| `pane.clear_agent_authority` | `pane_id`: string **req**<br>`seq`: integer|null/uint64<br>`source`: string|null | `pane_id` |
| `pane.close` | `pane_id`: string **req** | `pane_id` |
| `pane.copy_motion` | `content_revision`: integer|null/uint64<br>`cursor`: PaneTextPoint **req**<br>`motion`: PaneCopyMotion **req**<br>`pane_id`: string **req** | `pane_id`, `cursor`, `motion` |
| `pane.copy_search` | `content_revision`: integer/uint64 **req**<br>`cursor`: PaneTextPoint **req**<br>`direction`: PaneCopySearchDirection **req**<br>`pane_id`: string **req**<br>`previous`: PaneTextRange<br>`query`: string **req** | `pane_id`, `query`, `direction`, `cursor`, `content_revision` |
| `pane.current` | `caller_pane_id`: string|null | — |
| `pane.edges` | `pane_id`: string|null | — |
| `pane.edit_scrollback` | `pane_id`: string **req** | `pane_id` |
| `pane.focus` | `pane_id`: string **req** | `pane_id` |
| `pane.focus_direction` | `direction`: PaneDirection **req**<br>`pane_id`: string|null | `direction` |
| `pane.get` | `pane_id`: string **req** | `pane_id` |
| `pane.input.set` | `pane_id`: string **req**<br>`right_click`: PaneRightClickTarget **req** | `pane_id`, `right_click` |
| `pane.layout` | `pane_id`: string|null | — |
| `pane.link.activate` | `col`: integer/uint16 **req**<br>`content_revision`: integer|null/uint64<br>`offset_from_bottom`: integer|null/uint64<br>`pane_id`: string **req**<br>`viewport_row`: integer/uint16 **req** | `pane_id`, `viewport_row`, `col` |
| `pane.link.resolve` | `col`: integer/uint16 **req**<br>`content_revision`: integer|null/uint64<br>`offset_from_bottom`: integer|null/uint64<br>`pane_id`: string **req**<br>`viewport_row`: integer/uint16 **req** | `pane_id`, `viewport_row`, `col` |
| `pane.list` | `workspace_id`: string|null | — |
| `pane.move` | `destination`: PaneMoveDestination **req**<br>`focus`: boolean<br>`pane_id`: string **req** | `pane_id`, `destination` |
| `pane.neighbor` | `direction`: PaneDirection **req**<br>`pane_id`: string|null | `direction` |
| `pane.process_info` | `pane_id`: string|null | — |
| `pane.read` | `format`: ReadFormat<br>`lines`: integer|null/uint32<br>`pane_id`: string **req**<br>`source`: ReadSource **req**<br>`strip_ansi`: boolean | `pane_id`, `source` |
| `pane.release_agent` | `agent`: string **req**<br>`pane_id`: string **req**<br>`seq`: integer|null/uint64<br>`source`: string **req** | `pane_id`, `source`, `agent` |
| `pane.rename` | `label`: string|null<br>`pane_id`: string **req** | `pane_id` |
| `pane.report_agent` | `agent`: string **req**<br>`agent_session_id`: string|null<br>`agent_session_path`: string|null<br>`message`: string|null<br>`pane_id`: string **req**<br>`resume_argv`: array|null<br>`seq`: integer|null/uint64<br>`source`: string **req**<br>`state`: PaneAgentState **req** | `pane_id`, `source`, `agent`, `state` |
| `pane.report_agent_session` | `agent`: string **req**<br>`agent_session_id`: string|null<br>`agent_session_path`: string|null<br>`pane_id`: string **req**<br>`resume_argv`: array|null<br>`seq`: integer|null/uint64<br>`session_start_source`: string|null<br>`source`: string **req** | `pane_id`, `source`, `agent` |
| `pane.report_metadata` | `agent`: string|null<br>`applies_to_source`: string|null<br>`clear_display_agent`: boolean<br>`clear_state_labels`: boolean<br>`clear_title`: boolean<br>`display_agent`: string|null<br>`pane_id`: string **req**<br>`seq`: integer|null/uint64<br>`source`: string **req**<br>`state_labels`: object<br>`title`: string|null<br>`tokens`: object<br>`ttl_ms`: integer|null/uint64 | `pane_id`, `source` |
| `pane.resize` | `amount`: number|null/float<br>`direction`: PaneDirection **req**<br>`pane_id`: string|null | `direction` |
| `pane.scroll` | `offset_from_bottom`: integer/uint64 **req**<br>`pane_id`: string **req** | `pane_id`, `offset_from_bottom` |
| `pane.selection.read` | `anchor`: PaneTextPoint **req**<br>`content_revision`: integer|null/uint64<br>`cursor`: PaneTextPoint **req**<br>`pane_id`: string **req** | `pane_id`, `anchor`, `cursor` |
| `pane.send_input` | `keys`: array<br>`pane_id`: string **req**<br>`text`: string | `pane_id` |
| `pane.send_keys` | `keys`: array **req**<br>`pane_id`: string **req** | `pane_id`, `keys` |
| `pane.send_text` | `pane_id`: string **req**<br>`text`: string **req** | `pane_id`, `text` |
| `pane.split` | `cwd`: string|null<br>`direction`: SplitDirection **req**<br>`env`: object<br>`focus`: boolean<br>`ratio`: number|null/float<br>`right_click`: PaneRightClickTarget<br>`target_pane_id`: string|null<br>`workspace_id`: string|null | `direction` |
| `pane.swap` | `direction`: PaneDirection<br>`pane_id`: string|null<br>`source_pane_id`: string|null<br>`target_pane_id`: string|null | — |
| `pane.wait_for_output` | `lines`: integer|null/uint32<br>`match`: OutputMatch **req**<br>`pane_id`: string **req**<br>`source`: ReadSource **req**<br>`strip_ansi`: boolean<br>`timeout_ms`: integer|null/uint64 | `pane_id`, `source`, `match` |
| `pane.zoom` | `mode`: PaneZoomMode<br>`pane_id`: string|null | — |

## `ping.*`

| Method | Params | Required |
|---|---|---|
| `ping` | — | — |

## `plugin.*`

| Method | Params | Required |
|---|---|---|
| `plugin.action.invoke` | `action_id`: string **req**<br>`context`: PluginInvocationContext<br>`plugin_id`: string|null | `action_id` |
| `plugin.action.list` | `plugin_id`: string|null | — |
| `plugin.disable` | `plugin_id`: string **req** | `plugin_id` |
| `plugin.enable` | `plugin_id`: string **req** | `plugin_id` |
| `plugin.link` | `enabled`: boolean<br>`path`: string **req**<br>`source`: PluginSourceInfo | `path` |
| `plugin.list` | `plugin_id`: string|null | — |
| `plugin.log.list` | `limit`: integer|null/uint<br>`plugin_id`: string|null | — |
| `plugin.pane.close` | `pane_id`: string **req** | `pane_id` |
| `plugin.pane.focus` | `pane_id`: string **req** | `pane_id` |
| `plugin.pane.open` | `cwd`: string|null<br>`direction`: SplitDirection<br>`entrypoint`: string **req**<br>`env`: object<br>`focus`: boolean<br>`height`: PopupSize<br>`placement`: PluginPanePlacement<br>`plugin_id`: string **req**<br>`target_pane_id`: string|null<br>`width`: PopupSize<br>`workspace_id`: string|null | `plugin_id`, `entrypoint` |
| `plugin.unlink` | `plugin_id`: string **req** | `plugin_id` |

## `popup.*`

| Method | Params | Required |
|---|---|---|
| `popup.close` | — | — |

## `product_announcement.*`

| Method | Params | Required |
|---|---|---|
| `product_announcement.dismiss` | `id`: string **req**<br>`version`: string **req** | `version`, `id` |

## `release_notes.*`

| Method | Params | Required |
|---|---|---|
| `release_notes.dismiss` | `version`: string **req** | `version` |

## `server.*`

| Method | Params | Required |
|---|---|---|
| `server.agent_manifests` | — | — |
| `server.live_handoff` | `expected_protocol`: integer|null/uint32<br>`expected_version`: string|null<br>`import_exe`: string|null | — |
| `server.reload_agent_manifests` | — | — |
| `server.reload_config` | — | — |
| `server.ssh_agent.register` | `socket_path`: string **req** | `socket_path` |
| `server.stop` | — | — |

## `session.*`

| Method | Params | Required |
|---|---|---|
| `session.snapshot` | — | — |

## `tab.*`

| Method | Params | Required |
|---|---|---|
| `tab.close` | `tab_id`: string **req** | `tab_id` |
| `tab.create` | `cwd`: string|null<br>`env`: object<br>`focus`: boolean<br>`label`: string|null<br>`workspace_id`: string|null | — |
| `tab.focus` | `tab_id`: string **req** | `tab_id` |
| `tab.get` | `tab_id`: string **req** | `tab_id` |
| `tab.list` | `workspace_id`: string|null | — |
| `tab.move` | `insert_index`: integer/uint **req**<br>`tab_id`: string **req** | `tab_id`, `insert_index` |
| `tab.rename` | `label`: string **req**<br>`tab_id`: string **req** | `tab_id`, `label` |

## `workspace.*`

| Method | Params | Required |
|---|---|---|
| `workspace.close` | `close_group`: boolean<br>`workspace_id`: string **req** | `workspace_id` |
| `workspace.create` | `cwd`: string|null<br>`env`: object<br>`focus`: boolean<br>`label`: string|null<br>`source_workspace_id`: string|null | — |
| `workspace.focus` | `workspace_id`: string **req** | `workspace_id` |
| `workspace.get` | `workspace_id`: string **req** | `workspace_id` |
| `workspace.list` | — | — |
| `workspace.move` | `insert_index`: integer/uint **req**<br>`workspace_id`: string **req** | `workspace_id`, `insert_index` |
| `workspace.move_block` | `before_workspace_id`: string|null<br>`workspace_ids`: array **req** | `workspace_ids` |
| `workspace.rename` | `label`: string **req**<br>`workspace_id`: string **req** | `workspace_id`, `label` |
| `workspace.report_metadata` | `seq`: integer|null/uint64<br>`source`: string **req**<br>`tokens`: object **req**<br>`ttl_ms`: integer|null/uint64<br>`workspace_id`: string **req** | `workspace_id`, `source`, `tokens` |

## `worktree.*`

| Method | Params | Required |
|---|---|---|
| `worktree.create` | `base`: string|null<br>`branch`: string|null<br>`cwd`: string|null<br>`focus`: boolean<br>`label`: string|null<br>`path`: string|null<br>`trust_repository`: boolean<br>`workspace_id`: string|null | — |
| `worktree.list` | `cwd`: string|null<br>`trust_repository`: boolean<br>`workspace_id`: string|null | — |
| `worktree.open` | `branch`: string|null<br>`cwd`: string|null<br>`focus`: boolean<br>`label`: string|null<br>`path`: string|null<br>`trust_repository`: boolean<br>`workspace_id`: string|null | — |
| `worktree.remove` | `force`: boolean<br>`trust_repository`: boolean<br>`workspace_id`: string **req** | `workspace_id` |

## Shared enums / types

| Type | Values |
|---|---|
| `AgentStatus` | `idle`, `working`, `blocked`, `done`, `unknown` |
| `AgentViewBuiltinField` | `status`, `workspace_id`, `tab_id`, `pane_id`, `agent`, `seen`, `state_change_seq` |
| `AgentViewBuiltinSortField` | `workspace_order`, `tab_order`, `pane_order`, `attention`, `status`, `agent`, `seen`, `state_change_seq` |
| `AgentViewContext` | `current_workspace_id`, `current_tab_id` |
| `AgentViewSortOrder` | `asc`, `desc` |
| `IntegrationTarget` | `pi`, `omp`, `claude`, `codex`, `copilot`, `devin`, `droid`, `kimi`, `opencode`, `kilo`, `hermes`, `qodercli`, `qwen`, `cursor`, `mastracode`, `antigravity_cli`, `grok` |
| `NotificationShowSound` | `none`, `done`, `request` |
| `PaneAgentState` | `idle`, `working`, `blocked`, `unknown` |
| `PaneCopyMotion` | `line_end`, `first_non_blank`, `next_word_start`, `previous_word_start`, `next_word_end`, `next_big_word_start`, `previous_big_word_start`, `next_big_word_end`, `previous_paragraph`, `next_paragraph` |
| `PaneCopySearchDirection` | `forward`, `backward` |
| `PaneDirection` | `left`, `right`, `up`, `down` |
| `PaneRightClickTarget` | `herdr`, `pane` |
| `PaneZoomMode` | `toggle`, `on`, `off` |
| `PluginPanePlacement` | `overlay`, `popup`, `split`, `tab`, `zoomed` |
| `PluginSourceKind` | `local`, `github` |
| `ReadFormat` | `text`, `ansi` |
| `ReadSource` | `visible`, `recent`, `recent_unwrapped`, `detection` |
| `SplitDirection` | `right`, `down` |
| `ToastHerdrPosition` | `top-left`, `top-right`, `bottom-left`, `bottom-right` |
