import { SHELL_HARNESS } from '@/features/app-tabs/data'
import type { LinkItem, ShellHarnessData, TopNavJoinCommand } from '@/features/app-tabs/types'
import type { StatusPayload } from '@/lib/api/types'
import { env } from '@/lib/env'
import { isPublicMesh } from '@/lib/api/mesh-visibility'

export type TopNavShellData = {
  apiUrl: string
  topNavApiAccessLinks: LinkItem[]
  topNavJoinCommands: TopNavJoinCommand[]
  topNavJoinLinks: LinkItem[]
}

const LOCAL_API_HOSTS = new Set(['localhost', '127.0.0.1', '0.0.0.0'])

function normalizeOpenAIBaseUrl(value: string) {
  const trimmed = value.trim().replace(/\/+$/, '')
  return trimmed.endsWith('/v1') ? trimmed : `${trimmed}/v1`
}

function isLocalApiUrl(value: string) {
  try {
    return LOCAL_API_HOSTS.has(new URL(value).hostname)
  } catch {
    return false
  }
}

function browserOrigin() {
  if (typeof window === 'undefined') return null
  return window.location.origin
}

export function resolveOpenAIBaseUrl(status?: StatusPayload) {
  if (typeof window !== 'undefined' && LOCAL_API_HOSTS.has(window.location.hostname)) {
    return normalizeOpenAIBaseUrl(`http://127.0.0.1:${status?.api_port ?? 9337}`)
  }

  if (env.isDevelopment) {
    return normalizeOpenAIBaseUrl(env.apiUrl)
  }

  const origin = browserOrigin()
  if (origin && isLocalApiUrl(env.apiUrl)) {
    return normalizeOpenAIBaseUrl(origin)
  }

  return normalizeOpenAIBaseUrl(env.apiUrl)
}

function buildPrivateJoinCommands(): TopNavJoinCommand[] {
  return [
    {
      label: 'Private mesh invitations',
      value: 'Invitation details are intentionally not shown in the console.',
      hint: 'Use a trusted local operator channel to issue or share a private-mesh invitation.',
      disabled: true
    },
    {
      label: 'Auto join and serve command',
      value: 'Private-mesh join command unavailable in the console',
      prefix: '$',
      hint: 'Get the join command from a trusted local operator channel.',
      disabled: true
    },
    {
      label: 'Client-only join command',
      value: 'Private-mesh client command unavailable in the console',
      prefix: '$',
      hint: 'Get the join command from a trusted local operator channel.',
      disabled: true
    }
  ]
}

function buildPublicJoinCommands(): TopNavJoinCommand[] {
  return [
    {
      label: 'Public mesh command',
      value: 'mesh-llm --auto',
      prefix: '$',
      hint: 'Join public discovery, auto-select a model, and serve the local API.'
    }
  ]
}

function buildUnavailableJoinCommands(): TopNavJoinCommand[] {
  return [
    {
      label: 'Private mesh invitations',
      value: 'Private-mesh invitation status unavailable',
      hint: 'Connect to a local mesh node to see safe invitation availability metadata.',
      disabled: true
    },
    {
      label: 'Auto join and serve command',
      value: 'Auto join command unavailable',
      prefix: '$',
      hint: 'Get private-mesh join commands from a trusted local operator channel.',
      disabled: true
    },
    {
      label: 'Client-only join command',
      value: 'Client-only join command unavailable',
      prefix: '$',
      hint: 'Get private-mesh join commands from a trusted local operator channel.',
      disabled: true
    }
  ]
}

export function resolveHarnessTopNavData(data: ShellHarnessData): TopNavShellData {
  return {
    apiUrl: env.apiUrl,
    topNavApiAccessLinks: data.topNavApiAccessLinks,
    topNavJoinCommands: data.topNavJoinCommands,
    topNavJoinLinks: data.topNavJoinLinks
  }
}

export function resolveLiveTopNavData(status?: StatusPayload): TopNavShellData {
  const topNavJoinCommands =
    status && isPublicMesh(status)
      ? buildPublicJoinCommands()
      : status
        ? buildPrivateJoinCommands()
        : buildUnavailableJoinCommands()

  return {
    apiUrl: resolveOpenAIBaseUrl(status),
    topNavApiAccessLinks: SHELL_HARNESS.topNavApiAccessLinks,
    topNavJoinCommands,
    topNavJoinLinks: SHELL_HARNESS.topNavJoinLinks
  }
}
