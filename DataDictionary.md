### Data Dictionary
**Dimension tables**

1. **I94 People Table**

| Field Name | Description | Data Type |
| ----------- | ----------- | ----------- |
| Cicid | Id | Double |
| Gender | Gender | String |
| i94res | Country of residence | Double |
| i94cit | Country of citizenship | Double |
| bir_year | Year of Birth | Double |
| age | Age | Double |
| i94addr | State of resisdence in US | String |

2. **I94 Date table**
 
| Field Name | Description | Data Type |
| ----------- | ----------- | ----------- |
| Date | Id | Date |
| Day | Day of month | Int |
| Month | Month of year | Int |
| Year | Year | Int |

4. **Visa table**

| Field Name | Description | Data Type |
| ----------- | ----------- | ----------- |
| Visa_type | Visa type | Double |
| Purpose | Purpose of visit | String |

5. **AirportCodes Table**

| Field Name | Description | Data Type |
| ----------- | ----------- | ----------- |
| Iiata_code | Airport unique code | String |
| Name | Name of airport | String |
| region | US region/state | String |

Child table
1. **US States Demographics table**

| Field Name | Description | Data Type |
| ----------- | ----------- | ----------- |
| state_code | US State code | String |
| median_age | Avg age of the residents of the state | Double |
| total_population | Total Population of state | BigInt |

**Fact table**

1. **I94_immigration_table**

| Field Name | Description | Data Type |
| ----------- | ----------- | ----------- |
| Cicid | Id | Double |
| i94port | Port of entry | String |
| i94mon | Month of entry | Double |
| arrdate | Date of arrival | Double |
| deptdate | Date of departure | Double |
| visatype | Type of visa  | String |
| count | Number  | Double |
