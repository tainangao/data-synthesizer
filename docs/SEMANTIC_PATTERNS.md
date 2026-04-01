# Semantic Field Patterns Reference

This document lists the semantic patterns used by `value_generators.py` to automatically generate appropriate data based on column names.

## Numerical Fields

| Pattern | Distribution | Parameters | Example Values |
|---------|-------------|------------|----------------|
| `score`, `rating` | Gaussian | mean=650, std=100 | 550, 720, 630 |
| `rate`, `yield`, `interest` | Gaussian | mean=5.5, std=3.0 | 3.2%, 7.8%, 4.5% |
| `amount`, `balance`, `principal` | Lognormal | mu=8, sigma=1.2 | 8500, 45000, 12000 |
| `age` | Gaussian (int) | mean=35, std=12 | 28, 42, 31 |
| `quantity`, `count` | Lognormal (int) | mean=2, sigma=0.5 | 3, 7, 5 |

## Categorical Fields

| Pattern | Categories | Weights | Example Values |
|---------|-----------|---------|----------------|
| `status`, `state` | Active, Inactive, Pending, Closed | 70, 10, 12, 8 | Active, Pending |
| `profile`, `behavior` | Conservative, Moderate, Aggressive | 50, 35, 15 | Conservative, Moderate |
| `segment` | Mass, Affluent, Premium | 60, 30, 10 | Mass, Affluent |
| `risk` | Low, Medium, High | 60, 30, 10 | Low, Medium |
| `type` | Standard, Premium, Enterprise | 60, 30, 10 | Standard, Premium |
| `currency` | USD, EUR, GBP, JPY | Equal | USD, EUR |
| `country` | ISO codes | Equal | US, GB, FR |

## Text Fields (Faker-based)

| Pattern | Generator | Example Values |
|---------|-----------|----------------|
| `first_name` | Faker.first_name() | John, Sarah, Michael |
| `last_name` | Faker.last_name() | Smith, Johnson, Williams |
| `name` | Faker.name() | John Smith, Sarah Johnson |
| `email` | Faker.email() | john@example.com |
| `phone` | Faker.phone_number() | +1-555-123-4567 |
| `address` | Faker.address() | 123 Main St, City, State |
| `city` | Faker.city() | New York, London |
| `company` | Faker.company() | Acme Corp, Tech Inc |
| `description`, `comment` | Faker.sentence() | This is a sample sentence. |

## Temporal Fields

| Pattern | Logic | Example Values |
|---------|-------|----------------|
| `birth`, `dob` | Current date - age*365 | 1988-03-15, 1992-07-22 |
| `end`, `close`, `maturity` | anchor + 30-365 days | 2024-06-15 (if start=2024-01-01) |
| `due`, `scheduled` | anchor + 1-90 days | 2024-02-15 (if start=2024-01-01) |
| Other temporal | Random within simulation range | 2023-05-10, 2024-11-20 |

**Special behaviors:**
- If `anchor_series` provided: generates relative to anchor date
- If `parent_temporal` provided: ensures child date >= parent date

## Semi-Structured Fields (JSON/XML)

| Pattern | Structure | Example |
|---------|-----------|---------|
| `preference`, `settings` | JSON with language, notifications, theme, marketing_opt_in | `{"language": "en", "notifications": true, "marketing_opt_in": false}` |
| `risk_model` | JSON with score, tier, factors, model_version | `{"score": 650, "tier": "Medium", "model_version": "v2.3"}` |
| `address`, `location` | JSON with street, city, country, postal_code | `{"street": "123 Main", "city": "NYC", "postal_code": "10001"}` |
| `metadata`, `attribute` | JSON with source, created_by, tags | `{"source": "web", "tags": ["new"]}` |
| Default JSON | JSON with value, count, active | `{"value": "word", "count": 5, "active": true}` |

## XML Patterns

| Pattern | Structure | Example |
|---------|-----------|---------|
| `transaction`, `payment` | XML with amount, channel, reference | `<transaction><amount currency="USD">1234.56</amount><channel>online</channel></transaction>` |
| `risk`, `score` | XML with score, grade, factors | `<risk_assessment><score>650</score><grade>B</grade><factors>...</factors></risk_assessment>` |
| `profile`, `customer` | XML with segment, channel, since | `<profile><segment>Retail</segment><channel>online</channel></profile>` |
| `order`, `trade` | XML with instrument, quantity, price | `<order_details><instrument>AAPL</instrument><quantity>100</quantity></order_details>` |
| Default XML | XML with id, value, status | `<data><id>AB-123456</id><value>word</value><status>active</status></data>` |

## Boolean Fields

| Pattern | True Weight | False Weight | Example |
|---------|------------|--------------|---------|
| `active`, `enabled` | 80% | 20% | true, true, false |
| `default` | 20% | 80% | false, false, true |
| Other | 50% | 50% | true, false |

## Inheritance Patterns

Fields with these tokens will attempt to inherit from parent tables via FK:

- `currency` - Inherits currency from parent entity
- `country` - Inherits country from parent entity
- `segment` - Inherits customer segment
- `risk` - Inherits risk classification
- `type` - Inherits type/category
- `channel` - Inherits channel/source
- `status` - Can inherit status (context-dependent)
- `grade` - Inherits grade/rating

**Example:** If `Transaction` has FK to `Account`, and both have a `currency` field, Transaction.currency will copy Account.currency values.
