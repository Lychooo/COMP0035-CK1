```mermaid
erDiagram
    UNIVERSITY ||--o{ PROGRAMME : has
    PROGRAMME  ||--o{ SURVEY_RESULT : has
    SURVEY_YEAR ||--o{ SURVEY_RESULT : in

    UNIVERSITY {
        int     university_id PK
        string  name  "UNIQUE, NOT NULL"
        string  short_name "UNIQUE, NULL"
    }

    PROGRAMME {
        int     programme_id PK
        int     university_id FK
        string  name "NOT NULL"
        "UNIQUE(university_id, name)"
    }

    SURVEY_YEAR {
        int       year_id PK
        smallint  year "UNIQUE, 2000..2100"
    }

    SURVEY_RESULT {
        int            result_id PK
        int            programme_id FK
        int            year_id FK
        numeric(5,2)   employment_rate_overall "0..100"
        numeric(5,2)   employment_rate_ft_perm  "0..100, <= overall"
        numeric(10,2)  basic_monthly_mean   ">= 0"
        numeric(10,2)  basic_monthly_median ">= 0"
        numeric(10,2)  gross_monthly_mean   ">= basic"
        numeric(10,2)  gross_monthly_median ">= basic"
        "UNIQUE(programme_id, year_id)"
    }
```