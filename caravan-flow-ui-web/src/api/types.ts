// Wire types for the management API. Kept loose (records + unions)
// rather than generated — the worker's JSON shapes live in
// CaravanFlow.Core on the C# side and caravanflow.api on the Java
// side; a schema-share slice can replace these with generated types
// later.

export type ProcessorState = 'ENABLED' | 'DISABLED' | 'STOPPED'

export interface ProcessorStats {
  processed: number
  errors: number
}

export interface Processor {
  name: string
  type: string
  state: ProcessorState
  stats?: ProcessorStats
  config?: Record<string, unknown>
  connections?: Record<string, string[]>
}

export interface Source {
  name: string
  type: string
  running: boolean
}

export interface Provider {
  name: string
  type: string
  state: string
}

export interface Flow {
  processors: Processor[]
  entryPoints: string[]
  sources: Source[]
  providers: Provider[]
  stats: {
    processed: number
    activeExecutions: number
    processors: number
    sources?: number
  }
}

export interface ProcessorStatsMap {
  [name: string]: ProcessorStats
}

export interface VcStatus {
  enabled: boolean
  clean?: boolean
  branch?: string
  ahead?: number
  behind?: number
  error?: string
}

export interface ProvenanceEvent {
  timestamp: number
  type: string
  flowfile: string
  component: string
  details: string
}

export interface SourceInfo {
  name: string
  type: string
  running: boolean
}

export type ParamKind =
  | 'String'
  | 'Multiline'
  | 'Integer'
  | 'Number'
  | 'Boolean'
  | 'Enum'
  | 'Expression'
  | 'KeyValueList'
  | 'StringList'
  | 'Secret'

export interface ParamInfo {
  name: string
  label: string
  description: string
  kind: ParamKind
  required: boolean
  default: string | null
  placeholder: string | null
  choices: string[] | null
  valueKind: ParamKind | null
  entryDelim: string
  pairDelim: string
}

export interface RegistryEntry {
  name: string
  description: string
  category?: string
  configKeys: string[]
  parameters?: ParamInfo[]
}
