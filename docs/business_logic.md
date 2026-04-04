# Business Logic Reference for Synthetic Data Generation

This document provides guidance for mapping schemas to realistic behavioral patterns. Use these as templates when matching patterns are detected, but adapt and extend based on the specific schema provided.

---

## Scenario 1: CRM / Customer Lifecycle

### Core Rules
- **Temporal hierarchy**: `CustomerJoinDate` ≤ `AccountOpenDate` ≤ `TransactionDate`
- **Interaction timing**: `InteractionDate` ≥ `CustomerJoinDate` (can occur before account opening)
- **Account lifecycle**: `Pending` → `Active` → `Dormant` (optional) → `Closed`
- **Integrity**: No transactions after `AccountCloseDate`
- **Balance**: `CurrentBalance` = Σ(Credits) - Σ(Debits)

### Customer Segmentation
- Segment types: Retail (80%), Mass Affluent (15%), HNW (5%)
- Age: Normal(35, 12)
- Income: LogNormal distribution
- Acquisition channels: Online / Branch / Referral

### Transaction Behavior
- Frequency: Poisson(λ), where λ depends on segment, income, account age
- Balance patterns: monthly salary credits, daily/weekly debits
- Dormancy trigger: no transactions for >90 days

### Account Status Transitions (Base Probabilities)

| From → To | Pending | Active | Dormant | Closed/Rejected |
|-----------|---------|--------|---------|-----------------|
| Pending   | 10%     | 85%    | 0%      | 5%              |
| Active    | 0%      | 85%    | 14%     | 1%              |
| Dormant   | 0%      | 10%    | 80%     | 10%             |
| Closed    | 0%      | 0%     | 0%      | 100%            |

**Adjustments**: Increase P(Active → Dormant) when activity decreases; increase P(Dormant → Closed) when balance is low.

---

## Scenario 2: Trading / Market Execution

### Core Rules
- **Trade lifecycle**: `Order` → `Execution` → `Settlement`
- **T+2 settlement**: `SettlementDate` = `ExecutionDate` + 2 business days
- **Execution constraint**: `ExecutedQuantity` ≤ `OrderedQuantity`
- **Portfolio impact**: Buy increases position, Sell decreases position

### Trader Segmentation
- Retail: small, infrequent trades
- Institutional: large, frequent trades
- HFT: many small trades

### Order Behavior
- Order frequency: Poisson(λ), where λ depends on trader type
- Order size: LogNormal distribution
- Price simulation: Geometric Brownian Motion for realistic price paths

### Order State Transitions (Base Probabilities)

| From → To | Open | Partial Fill | Filled | Cancelled |
|-----------|------|--------------|--------|-----------|
| Open      | 40%  | 20%          | 30%    | 10%       |
| Partial   | 0%   | 30%          | 60%    | 10%       |
| Filled    | 0%   | 0%           | 100%   | 0%        |
| Cancelled | 0%   | 0%           | 0%     | 100%      |

**Adjustments**: Condition transitions on liquidity, price movement, order urgency.

---

## Scenario 3: Credit Risk / Loan Repayment

### Core Rules
- **Loan lifecycle**: `OriginationDate` < `FirstPaymentDate` < `MaturityDate`
- **Repayment**: Monthly payments reduce `RemainingPrincipal`
- **Delinquency**: `PaymentDate` > `ScheduledDueDate` increases `DaysPastDue`
- **Happy path**: Current → Paid in Full → Closed
- **Stress path**: Current → Delinquent → Default → Charged-off
- **Terminal states**: No payments after Paid in Full or Charged-off

### Borrower Risk Profile
- Credit score: Normal(650, 100)
- Debt-to-income ratio (DTI)
- Loan amount and interest rate correlation

### Payment Patterns
- EMI-based repayment schedule
- Allow partial payments, missed payments, early repayment

### Loan Status Transitions (Base Probabilities)

| From → To | Current | Delinquent | Default | Paid in Full | Charged-off |
|-----------|---------|------------|---------|--------------|-------------|
| Current   | 94%     | 3%         | 0%      | 3%           | 0%          |
| Delinquent| 20%     | 60%        | 15%     | 5%           | 0%          |
| Default   | 5%      | 10%        | 70%     | 0%           | 15%         |
| Paid Full | 0%      | 0%         | 0%      | 100%         | 0%          |
| Charged-off| 0%     | 0%         | 0%      | 0%           | 100%        |

**Adjustments**: Increase P(Default) when credit score is low, DTI is high, or loan age increases.

---

## General Principles

### Entity Generation
1. Generate entities with demographic/risk profiles
2. Assign features that drive behavior (segment, score, type)
3. Initialize state machines with appropriate starting states

### Event-Based Simulation
- Update entity states based on transition probabilities
- Generate events (transactions, orders, payments) when entities are in eligible states
- Apply feature-based adjustments to base probabilities

### Transition Adjustments
- **higher_increases**: multiply base_prob by (1 + strength × normalized_field_value)
- **higher_decreases**: multiply base_prob by (1 - strength × normalized_field_value)
- Strength factors: weak=0.2, moderate=0.5, strong=1.0
- Re-normalize probabilities after adjustments

### Event Frequency
- Use Poisson distribution for event counts
- Modify λ based on entity features
- Example: high-income customers generate more transactions

### Constraints
- **temporal_order**: ensure date fields follow logical sequence
- **no_events_after_terminal**: drop events after entity reaches terminal state
- **running_balance**: maintain cumulative balance = credits - debits

### Realism Enhancements
- Heterogeneity: each entity has unique behavior profile
- Temporal drift: activity/risk evolves over time
- Seasonality: monthly cycles, market volatility spikes
- Noise: occasional missing values, delayed updates

---

## Instructions for Config Generation

When given a schema:
1. Identify entity tables (customers, accounts, loans, orders) vs event tables (transactions, interactions, payments)
2. Match entity patterns to scenarios above
3. Define state machines for entities with lifecycle states
4. Define event emission rules for event tables
5. Set up feature-based transition adjustments
6. Apply appropriate constraints
7. Use realistic distributions and parameters
8. **Extend beyond these templates** when the schema suggests additional patterns or business logic
