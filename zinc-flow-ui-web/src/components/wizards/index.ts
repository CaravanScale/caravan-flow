import type { ComponentType } from 'react'
import { TransformRecordWizard } from './TransformRecordWizard'
import { RouteOnAttributeWizard } from './RouteOnAttributeWizard'
import { ExtractRecordFieldWizard } from './ExtractRecordFieldWizard'
import { QueryRecordWizard } from './QueryRecordWizard'
import { RouteRecordWizard } from './RouteRecordWizard'
import { EvaluateExpressionWizard, UpdateRecordWizard } from './ComputeWizards'
import { RecordFieldsWizard } from './RecordFieldsWizard'
import { SplitTextWizard } from './SplitTextWizard'

// Processor-level wizards. Registry entries with
// `wizardComponent: "TransformRecord"` (etc.) render the named wizard
// instead of the generic per-param form. Wizards own the entire
// processor's config shape (not just one param) — they encapsulate the
// mini-DSL serialize/parse for their processor type.
//
// Contract: each wizard takes `values` (map of param name → string) and
// `onChange` (receives the next map). Saving is handled by the outer
// drawer; the wizard just edits the values in place.
export interface WizardProps {
  processorName: string
  values: Record<string, string>
  onChange: (next: Record<string, string>) => void
}

export const wizardRegistry: Record<string, ComponentType<WizardProps>> = {
  TransformRecord: TransformRecordWizard,
  RouteOnAttribute: RouteOnAttributeWizard,
  ExtractRecordField: ExtractRecordFieldWizard,
  QueryRecord: QueryRecordWizard,
  RouteRecord: RouteRecordWizard,
  EvaluateExpression: EvaluateExpressionWizard,
  UpdateRecord: UpdateRecordWizard,
  RecordFields: RecordFieldsWizard,
  SplitText: SplitTextWizard,
}
