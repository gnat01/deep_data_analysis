# Question Catalog

This catalog annotates the question bank into reusable KB-helper families.

## Fields

- `question_family`: the reusable analytical pattern
- `expected_output_shape`: the answer contract the helper should satisfy
- `evaluation_mode`: how correctness should be judged
- `primary_helper`: the main KB helper Task 3 should eventually implement

## Annotated Questions

### Q1

- question: In which months did DuckDuckGo post on HN hiring threads between January 2023 and April 2026?
- family: `company_activity_timeline`
- difficulty: `medium`
- primary entity: `company`
- time scope: `range`
- expected output: `ordered_month_list_with_counts_and_evidence`
- primary helper: `company_activity_postgres`
- secondary helper: `search_postgres_posts`
- evidence required: `high`
- evaluation mode: `set_match_plus_evidence`
- notes: Answer should include ordered months and at least a few source-linked posts.

### Q2

- question: Was there any month between December 2024 and January 2026 when DuckDuckGo was hiring for data science?
- family: `company_role_presence`
- difficulty: `medium`
- primary entity: `company_role`
- time scope: `range`
- expected output: `boolean_plus_matched_months_plus_evidence`
- primary helper: `company_role_presence_postgres`
- secondary helper: `search_postgres_roles`
- evidence required: `high`
- evaluation mode: `boolean_plus_month_set`
- notes: Best for yes/no questions about a company hiring for a role or concept in a date range.

### Q3

- question: Which companies posted hiring ads in every month of 2025?
- family: `general_analytical_lookup`
- difficulty: `hard`
- primary entity: `mixed`
- time scope: `all_time`
- expected output: `custom_analysis`
- primary helper: `composite_question_router`
- secondary helper: `search_postgres_posts`
- evidence required: `high`
- evaluation mode: `manual_review`
- notes: Likely needs composition of multiple helpers.

### Q4

- question: Which companies appeared for the first time in the HN hiring threads in 2026?
- family: `company_activity_ranking`
- difficulty: `medium`
- primary entity: `company`
- time scope: `slice`
- expected output: `ranked_table`
- primary helper: `company_activity_ranking_postgres`
- secondary helper: `month_summary_postgres`
- evidence required: `medium`
- evaluation mode: `ranked_top_k`
- notes: Ranking answers should expose the sorting metric clearly.

### Q5

- question: Which companies returned to hiring after being absent for at least 6 months?
- family: `company_activity_ranking`
- difficulty: `medium`
- primary entity: `company`
- time scope: `all_time`
- expected output: `ranked_table`
- primary helper: `company_activity_ranking_postgres`
- secondary helper: `month_summary_postgres`
- evidence required: `medium`
- evaluation mode: `ranked_top_k`
- notes: Ranking answers should expose the sorting metric clearly.

### Q6

- question: Which month had the highest number of distinct companies posting?
- family: `month_summary_ranking`
- difficulty: `medium`
- primary entity: `month`
- time scope: `all_time`
- expected output: `ranked_month_table`
- primary helper: `month_summary_postgres`
- secondary helper: `none`
- evidence required: `medium`
- evaluation mode: `exact_or_ranked_top_k`
- notes: Good candidate for deterministic aggregate SQL.

### Q7

- question: Which month had the highest number of distinct roles posted?
- family: `month_summary_ranking`
- difficulty: `medium`
- primary entity: `month`
- time scope: `all_time`
- expected output: `ranked_month_table`
- primary helper: `month_summary_postgres`
- secondary helper: `none`
- evidence required: `medium`
- evaluation mode: `exact_or_ranked_top_k`
- notes: Good candidate for deterministic aggregate SQL.

### Q8

- question: How did the share of remote roles change from 2023 to 2026?
- family: `company_role_presence`
- difficulty: `medium`
- primary entity: `company_role`
- time scope: `range`
- expected output: `boolean_plus_matched_months_plus_evidence`
- primary helper: `company_role_presence_postgres`
- secondary helper: `search_postgres_roles`
- evidence required: `high`
- evaluation mode: `boolean_plus_month_set`
- notes: Best for yes/no questions about a company hiring for a role or concept in a date range.

### Q9

- question: Which companies were remote-first in 2025?
- family: `general_analytical_lookup`
- difficulty: `hard`
- primary entity: `mixed`
- time scope: `slice`
- expected output: `custom_analysis`
- primary helper: `composite_question_router`
- secondary helper: `search_postgres_posts`
- evidence required: `high`
- evaluation mode: `manual_review`
- notes: Likely needs composition of multiple helpers.

### Q10

- question: Which companies shifted from remote to hybrid or onsite over time?
- family: `remote_mix_analysis`
- difficulty: `medium`
- primary entity: `remote_status`
- time scope: `range`
- expected output: `timeseries_or_distribution_table`
- primary helper: `remote_mix_postgres`
- secondary helper: `company_remote_mix_postgres`
- evidence required: `medium`
- evaluation mode: `aggregate_value_check`
- notes: Answers should clarify denominator and whether counts are posts or roles.

### Q11

- question: Which companies explicitly mentioned visa sponsorship?
- family: `attribute_presence`
- difficulty: `easy`
- primary entity: `attribute`
- time scope: `all_time`
- expected output: `company_list_with_evidence`
- primary helper: `attribute_presence_postgres`
- secondary helper: `search_postgres_posts`
- evidence required: `high`
- evaluation mode: `set_match_plus_evidence`
- notes: Good fit for visa and similar explicit mentions.

### Q12

- question: Which companies mentioned compensation most consistently across their posts?
- family: `compensation_history`
- difficulty: `hard`
- primary entity: `compensation`
- time scope: `all_time`
- expected output: `timeline_with_bands_and_evidence`
- primary helper: `compensation_history_postgres`
- secondary helper: `company_activity_postgres`
- evidence required: `high`
- evaluation mode: `timeline_with_manual_review`
- notes: Compensation is noisier than company/month facts and needs evidence rows.

### Q13

- question: Which companies showed the widest variation in hiring-post language across time?
- family: `company_change_analysis`
- difficulty: `hard`
- primary entity: `company_change`
- time scope: `all_time`
- expected output: `ranked_table_or_metric_summary_with_evidence`
- primary helper: `company_change_summary_postgres`
- secondary helper: `changed_companies_postgres`
- evidence required: `high`
- evaluation mode: `heuristic_review`
- notes: Use metrics plus evidence rows; avoid pretending these are crisp facts.

### Q14

- question: Which companies showed the least variation and mostly repeated the same post?
- family: `company_change_analysis`
- difficulty: `hard`
- primary entity: `company_change`
- time scope: `all_time`
- expected output: `ranked_table_or_metric_summary_with_evidence`
- primary helper: `company_change_summary_postgres`
- secondary helper: `changed_companies_postgres`
- evidence required: `high`
- evaluation mode: `heuristic_review`
- notes: Use metrics plus evidence rows; avoid pretending these are crisp facts.

### Q15

- question: For a selected company, how did post semantic spread compare with role semantic spread over time?
- family: `company_role_presence`
- difficulty: `medium`
- primary entity: `company_role`
- time scope: `range`
- expected output: `boolean_plus_matched_months_plus_evidence`
- primary helper: `company_role_presence_postgres`
- secondary helper: `search_postgres_roles`
- evidence required: `high`
- evaluation mode: `boolean_plus_month_set`
- notes: Best for yes/no questions about a company hiring for a role or concept in a date range.

### Q16

- question: Which companies had high role spread but low post spread?
- family: `general_analytical_lookup`
- difficulty: `hard`
- primary entity: `mixed`
- time scope: `all_time`
- expected output: `custom_analysis`
- primary helper: `composite_question_router`
- secondary helper: `search_postgres_posts`
- evidence required: `high`
- evaluation mode: `manual_review`
- notes: Likely needs composition of multiple helpers.

### Q17

- question: Which companies had high post spread but relatively low role spread?
- family: `general_analytical_lookup`
- difficulty: `hard`
- primary entity: `mixed`
- time scope: `all_time`
- expected output: `custom_analysis`
- primary helper: `composite_question_router`
- secondary helper: `search_postgres_posts`
- evidence required: `high`
- evaluation mode: `manual_review`
- notes: Likely needs composition of multiple helpers.

### Q18

- question: Which companies had the highest drift score over the full history?
- family: `company_change_analysis`
- difficulty: `hard`
- primary entity: `company_change`
- time scope: `all_time`
- expected output: `ranked_table_or_metric_summary_with_evidence`
- primary helper: `company_change_summary_postgres`
- secondary helper: `changed_companies_postgres`
- evidence required: `high`
- evaluation mode: `heuristic_review`
- notes: Use metrics plus evidence rows; avoid pretending these are crisp facts.

### Q19

- question: Which companies materially changed their hiring narrative in 2025?
- family: `company_change_analysis`
- difficulty: `hard`
- primary entity: `company_change`
- time scope: `slice`
- expected output: `ranked_table_or_metric_summary_with_evidence`
- primary helper: `company_change_summary_postgres`
- secondary helper: `changed_companies_postgres`
- evidence required: `high`
- evaluation mode: `heuristic_review`
- notes: Use metrics plus evidence rows; avoid pretending these are crisp facts.

### Q20

- question: Which companies seemed to pivot and then return to an earlier hiring pattern?
- family: `company_change_analysis`
- difficulty: `hard`
- primary entity: `company_change`
- time scope: `all_time`
- expected output: `ranked_table_or_metric_summary_with_evidence`
- primary helper: `company_change_summary_postgres`
- secondary helper: `changed_companies_postgres`
- evidence required: `high`
- evaluation mode: `heuristic_review`
- notes: Use metrics plus evidence rows; avoid pretending these are crisp facts.

### Q21

- question: Which role families grew the most between 2023 and 2026?
- family: `role_family_trend`
- difficulty: `medium`
- primary entity: `role_family`
- time scope: `range`
- expected output: `timeseries_or_ranked_trend_table`
- primary helper: `role_family_timeline_postgres`
- secondary helper: `month_summary_postgres`
- evidence required: `medium`
- evaluation mode: `aggregate_value_check`
- notes: Trend answers should be explicit about the comparison window.

### Q22

- question: Which months saw the strongest increase in ML / AI roles?
- family: `role_family_trend`
- difficulty: `medium`
- primary entity: `role_family`
- time scope: `all_time`
- expected output: `timeseries_or_ranked_trend_table`
- primary helper: `role_family_timeline_postgres`
- secondary helper: `month_summary_postgres`
- evidence required: `medium`
- evaluation mode: `aggregate_value_check`
- notes: Trend answers should be explicit about the comparison window.

### Q23

- question: Which companies were hiring for platform engineers in 2025?
- family: `role_company_lookup`
- difficulty: `medium`
- primary entity: `role`
- time scope: `slice`
- expected output: `company_list_or_company_month_table`
- primary helper: `companies_for_role_postgres`
- secondary helper: `search_postgres_roles`
- evidence required: `high`
- evaluation mode: `set_match_plus_evidence`
- notes: Should return companies, months, and matched role rows.

### Q24

- question: Which companies were hiring for data engineers remotely in 2024?
- family: `role_company_lookup`
- difficulty: `medium`
- primary entity: `role`
- time scope: `slice`
- expected output: `company_list_or_company_month_table`
- primary helper: `companies_for_role_postgres`
- secondary helper: `search_postgres_roles`
- evidence required: `high`
- evaluation mode: `set_match_plus_evidence`
- notes: Should return companies, months, and matched role rows.

### Q25

- question: Which companies were hiring for design roles while also mentioning AI concepts?
- family: `role_company_lookup`
- difficulty: `medium`
- primary entity: `role`
- time scope: `all_time`
- expected output: `company_list_or_company_month_table`
- primary helper: `companies_for_role_postgres`
- secondary helper: `search_postgres_roles`
- evidence required: `high`
- evaluation mode: `set_match_plus_evidence`
- notes: Should return companies, months, and matched role rows.

### Q26

- question: Which companies hired for both backend and ML roles in the same month?
- family: `general_analytical_lookup`
- difficulty: `hard`
- primary entity: `mixed`
- time scope: `slice`
- expected output: `custom_analysis`
- primary helper: `composite_question_router`
- secondary helper: `search_postgres_posts`
- evidence required: `high`
- evaluation mode: `manual_review`
- notes: Likely needs composition of multiple helpers.

### Q27

- question: Which companies posted the largest number of distinct role families in a single month?
- family: `role_family_trend`
- difficulty: `medium`
- primary entity: `role_family`
- time scope: `slice`
- expected output: `timeseries_or_ranked_trend_table`
- primary helper: `role_family_timeline_postgres`
- secondary helper: `month_summary_postgres`
- evidence required: `medium`
- evaluation mode: `aggregate_value_check`
- notes: Trend answers should be explicit about the comparison window.

### Q28

- question: What are the most common roles DuckDuckGo has hired for across all its posts?
- family: `role_company_lookup`
- difficulty: `medium`
- primary entity: `role`
- time scope: `range`
- expected output: `company_list_or_company_month_table`
- primary helper: `companies_for_role_postgres`
- secondary helper: `search_postgres_roles`
- evidence required: `high`
- evaluation mode: `set_match_plus_evidence`
- notes: Should return companies, months, and matched role rows.

### Q29

- question: For Apple, which months did it post, and what kinds of roles was it hiring for?
- family: `company_activity_timeline`
- difficulty: `medium`
- primary entity: `company`
- time scope: `all_time`
- expected output: `ordered_month_list_with_counts_and_evidence`
- primary helper: `company_activity_postgres`
- secondary helper: `search_postgres_posts`
- evidence required: `high`
- evaluation mode: `set_match_plus_evidence`
- notes: Answer should include ordered months and at least a few source-linked posts.

### Q30

- question: Which companies explicitly mentioned MCP, tool use, agents, evals, or RAG in 2026?
- family: `ai_concept_trend`
- difficulty: `hard`
- primary entity: `ai_concept`
- time scope: `slice`
- expected output: `timeseries_with_matched_examples`
- primary helper: `ai_concept_timeline_postgres`
- secondary helper: `search_postgres_posts`
- evidence required: `high`
- evaluation mode: `trend_plus_evidence`
- notes: Concept-trend answers must stay grounded in the defined dictionary.

### Q31

- question: When did agent-related terminology start appearing meaningfully in the hiring corpus?
- family: `company_role_presence`
- difficulty: `medium`
- primary entity: `company_role`
- time scope: `all_time`
- expected output: `boolean_plus_matched_months_plus_evidence`
- primary helper: `company_role_presence_postgres`
- secondary helper: `search_postgres_roles`
- evidence required: `high`
- evaluation mode: `boolean_plus_month_set`
- notes: Best for yes/no questions about a company hiring for a role or concept in a date range.

### Q32

- question: Which role families adopted agent / tooling language first?
- family: `role_family_trend`
- difficulty: `medium`
- primary entity: `role_family`
- time scope: `all_time`
- expected output: `timeseries_or_ranked_trend_table`
- primary helper: `role_family_timeline_postgres`
- secondary helper: `month_summary_postgres`
- evidence required: `medium`
- evaluation mode: `aggregate_value_check`
- notes: Trend answers should be explicit about the comparison window.

### Q33

- question: Which companies mentioned GPT / LLM / generative AI in 2023?
- family: `ai_concept_trend`
- difficulty: `hard`
- primary entity: `ai_concept`
- time scope: `slice`
- expected output: `timeseries_with_matched_examples`
- primary helper: `ai_concept_timeline_postgres`
- secondary helper: `search_postgres_posts`
- evidence required: `high`
- evaluation mode: `trend_plus_evidence`
- notes: Concept-trend answers must stay grounded in the defined dictionary.

### Q34

- question: Which companies changed from generic software hiring language to explicit AI-agent language over time?
- family: `general_analytical_lookup`
- difficulty: `hard`
- primary entity: `mixed`
- time scope: `range`
- expected output: `custom_analysis`
- primary helper: `composite_question_router`
- secondary helper: `search_postgres_posts`
- evidence required: `high`
- evaluation mode: `manual_review`
- notes: Likely needs composition of multiple helpers.

### Q35

- question: Which product themes were most common in 2023 versus 2025 versus 2026 YTD?
- family: `company_theme_change`
- difficulty: `hard`
- primary entity: `company_theme`
- time scope: `cross_year_comparison`
- expected output: `timeline_or_ranked_change_table_with_evidence`
- primary helper: `company_theme_history_postgres`
- secondary helper: `company_change_summary_postgres`
- evidence required: `high`
- evaluation mode: `heuristic_review`
- notes: Theme-change questions are heuristic and should not be overclaimed.

### Q36

- question: Which companies appeared to change what they were building, based on theme shifts across posts?
- family: `company_theme_change`
- difficulty: `hard`
- primary entity: `company_theme`
- time scope: `all_time`
- expected output: `timeline_or_ranked_change_table_with_evidence`
- primary helper: `company_theme_history_postgres`
- secondary helper: `company_change_summary_postgres`
- evidence required: `high`
- evaluation mode: `heuristic_review`
- notes: Theme-change questions are heuristic and should not be overclaimed.

### Q37

- question: Which companies repeatedly advertised the same compensation band month after month?
- family: `compensation_history`
- difficulty: `hard`
- primary entity: `compensation`
- time scope: `all_time`
- expected output: `timeline_with_bands_and_evidence`
- primary helper: `compensation_history_postgres`
- secondary helper: `company_activity_postgres`
- evidence required: `high`
- evaluation mode: `timeline_with_manual_review`
- notes: Compensation is noisier than company/month facts and needs evidence rows.

### Q38

- question: Which companies introduced compensation information later after initially omitting it?
- family: `compensation_history`
- difficulty: `hard`
- primary entity: `compensation`
- time scope: `all_time`
- expected output: `timeline_with_bands_and_evidence`
- primary helper: `compensation_history_postgres`
- secondary helper: `company_activity_postgres`
- evidence required: `high`
- evaluation mode: `timeline_with_manual_review`
- notes: Compensation is noisier than company/month facts and needs evidence rows.

### Q39

- question: Show me evidence rows for companies hiring for privacy, browser, or search-related roles.
- family: `evidence_retrieval`
- difficulty: `easy`
- primary entity: `evidence`
- time scope: `all_time`
- expected output: `evidence_row_list`
- primary helper: `evidence_lookup_postgres`
- secondary helper: `search_postgres_posts`
- evidence required: `high`
- evaluation mode: `evidence_row_match`
- notes: These should optimize for source-linked recall rather than summary elegance.

### Q40

- question: For a chosen company, show the posts and roles that best explain why its change score is high or low.
- family: `evidence_retrieval`
- difficulty: `easy`
- primary entity: `evidence`
- time scope: `all_time`
- expected output: `evidence_row_list`
- primary helper: `evidence_lookup_postgres`
- secondary helper: `search_postgres_posts`
- evidence required: `high`
- evaluation mode: `evidence_row_match`
- notes: These should optimize for source-linked recall rather than summary elegance.

### Q41

- question: Did any of the FAANG companies ever post on HN?
- family: `existence_lookup`
- difficulty: `easy`
- primary entity: `existence`
- time scope: `all_time`
- expected output: `boolean_plus_short_evidence`
- primary helper: `existence_check_postgres`
- secondary helper: `search_postgres_posts`
- evidence required: `high`
- evaluation mode: `boolean_plus_evidence`
- notes: Simple yes/no answers still need supporting rows.

### Q42

- question: Did any companies change their compensation band for the same job role over time? If so, which ones?
- family: `company_role_presence`
- difficulty: `medium`
- primary entity: `company_role`
- time scope: `range`
- expected output: `boolean_plus_matched_months_plus_evidence`
- primary helper: `company_role_presence_postgres`
- secondary helper: `search_postgres_roles`
- evidence required: `high`
- evaluation mode: `boolean_plus_month_set`
- notes: Best for yes/no questions about a company hiring for a role or concept in a date range.

### Q43

- question: In 50 words or less, how did the requirements for an AI engineer change from 2024 - 2026?
- family: `requirement_change_summary`
- difficulty: `hard`
- primary entity: `role_requirements`
- time scope: `range`
- expected output: `crisp_summary_with_supporting_evidence`
- primary helper: `role_requirement_change_summary_postgres`
- secondary helper: `search_postgres_posts`
- evidence required: `high`
- evaluation mode: `summary_with_evidence_review`
- notes: These are concise synthesis questions. The answer should summarize changes cleanly, not reduce them to yes/no.

### Q44

- question: Which companies did not hire for any ML or AI roles for the entire duration of this dataset?
- family: `company_role_presence`
- difficulty: `medium`
- primary entity: `company_role`
- time scope: `all_time`
- expected output: `boolean_plus_matched_months_plus_evidence`
- primary helper: `company_role_presence_postgres`
- secondary helper: `search_postgres_roles`
- evidence required: `high`
- evaluation mode: `boolean_plus_month_set`
- notes: Best for yes/no questions about a company hiring for a role or concept in a date range.

### Q45

- question: Give the geographical distribution of where companies were headquartered for every year.
- family: `company_geography`
- difficulty: `hard`
- primary entity: `company_geography`
- time scope: `yearly`
- expected output: `distribution_table`
- primary helper: `company_geography_postgres`
- secondary helper: `none`
- evidence required: `medium`
- evaluation mode: `aggregate_value_check`
- notes: Requires care because headquarters may not be present in the current normalized schema.

### Q46

- question: What is the didtribution of global remote roles to overall remote roles across the dataset? Can you give this on a year by year basis?
- family: `general_analytical_lookup`
- difficulty: `hard`
- primary entity: `mixed`
- time scope: `yearly`
- expected output: `custom_analysis`
- primary helper: `composite_question_router`
- secondary helper: `search_postgres_posts`
- evidence required: `high`
- evaluation mode: `manual_review`
- notes: Likely needs composition of multiple helpers.

### Q47

- question: Do Indian companies prefer onsite, hybrid or remote roles when they post?
- family: `remote_mix_analysis`
- difficulty: `medium`
- primary entity: `remote_status`
- time scope: `all_time`
- expected output: `timeseries_or_distribution_table`
- primary helper: `remote_mix_postgres`
- secondary helper: `company_remote_mix_postgres`
- evidence required: `medium`
- evaluation mode: `aggregate_value_check`
- notes: Answers should clarify denominator and whether counts are posts or roles.

### Q48

- question: What does the distribution of post lengths look like on a year by year basis?
- family: `post_shape_analysis`
- difficulty: `medium`
- primary entity: `post`
- time scope: `yearly`
- expected output: `distribution_or_timeseries_table`
- primary helper: `post_shape_summary_postgres`
- secondary helper: `company_activity_postgres`
- evidence required: `medium`
- evaluation mode: `aggregate_value_check`
- notes: Length- and shape-based questions are straightforward once defined carefully.

### Q49

- question: Are there companies who wrote longer posts consistetly over the years, or is this not seen in the data?
- family: `post_shape_analysis`
- difficulty: `medium`
- primary entity: `post`
- time scope: `all_time`
- expected output: `distribution_or_timeseries_table`
- primary helper: `post_shape_summary_postgres`
- secondary helper: `company_activity_postgres`
- evidence required: `medium`
- evaluation mode: `aggregate_value_check`
- notes: Length- and shape-based questions are straightforward once defined carefully.

### Q50

- question: Of the companies hiring remotely in 2025, which ones pay the same regardless of location or which ones give location adjusted compensation?
- family: `compensation_history`
- difficulty: `hard`
- primary entity: `compensation`
- time scope: `slice`
- expected output: `timeline_with_bands_and_evidence`
- primary helper: `compensation_history_postgres`
- secondary helper: `company_activity_postgres`
- evidence required: `high`
- evaluation mode: `timeline_with_manual_review`
- notes: Compensation is noisier than company/month facts and needs evidence rows.

## Family Coverage

- `general_analytical_lookup`: 7
- `company_role_presence`: 6
- `company_change_analysis`: 5
- `compensation_history`: 4
- `role_company_lookup`: 4
- `role_family_trend`: 4
- `ai_concept_trend`: 2
- `company_activity_ranking`: 2
- `company_activity_timeline`: 2
- `company_theme_change`: 2
- `evidence_retrieval`: 2
- `month_summary_ranking`: 2
- `post_shape_analysis`: 2
- `remote_mix_analysis`: 2
- `attribute_presence`: 1
- `company_geography`: 1
- `existence_lookup`: 1
- `requirement_change_summary`: 1
