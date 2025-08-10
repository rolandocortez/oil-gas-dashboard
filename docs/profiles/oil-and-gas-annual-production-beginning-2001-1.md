# CSV Profile
- **Path:** `data/raw/oil-and-gas-annual-production-beginning-2001-1.csv`
- **Delimiter:** `,`
- **Approx. rows:** 225032
- **Columns (18):**

  - `API Well Number`
  - `County`
  - `Company Name`
  - `API Hole Number`
  - `Sidetrack Code`
  - `Completion Code`
  - `Well Type Code`
  - `Production Field`
  - `Well Status Code`
  - `Well Name`
  - `Town`
  - `Producing Formation`
  - `Months in Production`
  - `Gas Produced, Mcf`
  - `Water Produced, bbl`
  - `Oil Produced, bbl`
  - `Reporting Year`
  - `New Georeferenced Column`

## Sample (first 10 rows)

|   API Well Number | County      | Company Name          |   API Hole Number |   Sidetrack Code |   Completion Code | Well Type Code   | Production Field   | Well Status Code   | Well Name          | Town   | Producing Formation   |   Months in Production |   Gas Produced, Mcf |   Water Produced, bbl |   Oil Produced, bbl |   Reporting Year |   New Georeferenced Column |
|------------------:|:------------|:----------------------|------------------:|-----------------:|------------------:|:-----------------|:-------------------|:-------------------|:-------------------|:-------|:----------------------|-----------------------:|--------------------:|----------------------:|--------------------:|-----------------:|---------------------------:|
|    31009055570000 | Cattaraugus | Not Applicable        |             05557 |               00 |                00 | NL               | Not Applicable     | VP                 | Voided Permit      | UNK    | Chipmunk              |                    nan |                 nan |                   nan |                 nan |             2001 |                        nan |
|    31009133440000 | Cattaraugus | Iroquois Gas Corp.    |             13344 |               00 |                00 | OD               | Not Applicable     | PA                 | F.Johnson 1        | UNK    | Not Applicable        |                      0 |                   0 |                   nan |                 nan |             2001 |                        nan |
|    31009134270000 | Cattaraugus | Iroquois Gas Corp.    |             13427 |               00 |                00 | OD               | Not Applicable     | IN                 | L.Angore (Bridges) | UNK    | Not Applicable        |                      0 |                   0 |                   nan |                 nan |             2001 |                        nan |
|    31009134190000 | Cattaraugus | Iroquois Gas Corp.    |             13419 |               00 |                00 | OD               | Not Applicable     | IN                 | Koelzow            | UNK    | Not Applicable        |                      0 |                   0 |                   nan |                 nan |             2001 |                        nan |
|    31029154370000 | Erie        | Flint Oil & Gas, Inc. |             15437 |               00 |                00 | NL               | Not Applicable     | NR                 | Renaldo, P. A. 1   | UNK    | Not Applicable        |                    nan |                 nan |                   nan |                 nan |             2001 |                        nan |
|    31009133270000 | Cattaraugus | Iroquois Gas Corp.    |             13327 |               00 |                00 | OD               | Not Applicable     | PA                 | W.Moore            | UNK    | Not Applicable        |                      0 |                   0 |                   nan |                 nan |             2001 |                        nan |
|    31029034870000 | Erie        | Iroquois Gas Corp.    |             03487 |               00 |                00 | GD               | Not Applicable     | PA                 | Ewald Robert 1     | UNK    | Medina                |                      0 |                   0 |                   nan |                 nan |             2001 |                        nan |
|    31009134000000 | Cattaraugus | Iroquois Gas Corp.    |             13400 |               00 |                00 | OD               | Not Applicable     | IN                 | Kenworth           | UNK    | Not Applicable        |                      0 |                   0 |                   nan |                 nan |             2001 |                        nan |
|    31009134120000 | Cattaraugus | Iroquois Gas Corp.    |             13412 |               00 |                00 | OD               | Not Applicable     | IN                 | Moran              | UNK    | Not Applicable        |                      0 |                   0 |                   nan |                 nan |             2001 |                        nan |
|    31009133200000 | Cattaraugus | Iroquois Gas Corp.    |             13320 |               00 |                00 | OD               | Not Applicable     | PA                 | Hatch              | UNK    | Not Applicable        |                      0 |                   0 |                   nan |                 nan |             2001 |                        nan |

## Null count (in first 5k rows)
| column | nulls |
|---|---:|
| API Well Number | 0 |
| County | 0 |
| Company Name | 0 |
| API Hole Number | 0 |
| Sidetrack Code | 0 |
| Completion Code | 0 |
| Well Type Code | 0 |
| Production Field | 4 |
| Well Status Code | 0 |
| Well Name | 0 |
| Town | 4 |
| Producing Formation | 0 |
| Months in Production | 160 |
| Gas Produced, Mcf | 1199 |
| Water Produced, bbl | 796 |
| Oil Produced, bbl | 1620 |
| Reporting Year | 0 |
| New Georeferenced Column | 49 |
