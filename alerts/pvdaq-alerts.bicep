// Alert rule definitions for PVDAQ ingestion function (FR-009b)
// Deploy via: az deployment group create --resource-group <rg> --template-file pvdaq-alerts.bicep

@description('Application Insights resource name')
param appInsightsName string

@description('Action group resource ID for alert notifications')
param actionGroupId string

@description('Validation failure rate threshold (percentage)')
param validationFailureThresholdPercent int = 10

@description('Processing latency threshold (milliseconds)')
param latencyThresholdMs int = 300000 // 5 minutes

resource appInsights 'Microsoft.Insights/components@2020-02-02' existing = {
  name: appInsightsName
}

// Alert 1: Validation failure rate spike (>10% of records in an invocation)
resource validationFailureAlert 'Microsoft.Insights/scheduledQueryRules@2023-03-15-preview' = {
  name: 'pvdaq-validation-failure-spike'
  location: resourceGroup().location
  properties: {
    displayName: 'PVDAQ Validation Failure Spike'
    description: 'Triggers when validation failure rate exceeds ${validationFailureThresholdPercent}% of records in a single invocation.'
    severity: 2 // Warning
    enabled: true
    evaluationFrequency: 'PT5M'
    windowSize: 'PT15M'
    scopes: [
      appInsights.id
    ]
    criteria: {
      allOf: [
        {
          query: '''
            traces
            | where message contains "Invocation metrics"
            | extend metrics = parse_json(message)
            | extend failRate = todouble(metrics.number_invalid) / todouble(metrics.number_of_records_retrieved) * 100
            | where metrics.number_of_records_retrieved > 0 and failRate > ${validationFailureThresholdPercent}
          '''
          timeAggregation: 'Count'
          operator: 'GreaterThan'
          threshold: 0
        }
      ]
    }
    actions: {
      actionGroups: [
        actionGroupId
      ]
    }
  }
}

// Alert 2: Emission failure (Service Bus send error after retry exhaustion)
resource emissionFailureAlert 'Microsoft.Insights/scheduledQueryRules@2023-03-15-preview' = {
  name: 'pvdaq-emission-failure'
  location: resourceGroup().location
  properties: {
    displayName: 'PVDAQ Emission Failure'
    description: 'Triggers when any Service Bus emission fails after retry exhaustion.'
    severity: 1 // Error
    enabled: true
    evaluationFrequency: 'PT5M'
    windowSize: 'PT5M'
    scopes: [
      appInsights.id
    ]
    criteria: {
      allOf: [
        {
          query: '''
            traces
            | where severityLevel >= 3
            | where message contains "Error processing record" or message contains "emit_cloudevent" or message contains "emit_dead_letter"
          '''
          timeAggregation: 'Count'
          operator: 'GreaterThan'
          threshold: 0
        }
      ]
    }
    actions: {
      actionGroups: [
        actionGroupId
      ]
    }
  }
}

// Alert 3: Abnormal processing latency
resource latencyAlert 'Microsoft.Insights/scheduledQueryRules@2023-03-15-preview' = {
  name: 'pvdaq-abnormal-latency'
  location: resourceGroup().location
  properties: {
    displayName: 'PVDAQ Abnormal Processing Latency'
    description: 'Triggers when invocation duration exceeds ${latencyThresholdMs}ms.'
    severity: 2 // Warning
    enabled: true
    evaluationFrequency: 'PT5M'
    windowSize: 'PT15M'
    scopes: [
      appInsights.id
    ]
    criteria: {
      allOf: [
        {
          query: '''
            traces
            | where message contains "Invocation metrics"
            | extend metrics = parse_json(message)
            | where todouble(metrics.duration_ms) > ${latencyThresholdMs}
          '''
          timeAggregation: 'Count'
          operator: 'GreaterThan'
          threshold: 0
        }
      ]
    }
    actions: {
      actionGroups: [
        actionGroupId
      ]
    }
  }
}
