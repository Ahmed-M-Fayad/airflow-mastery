# 02 - API Incremental Ingestion

Second project in the Airflow mastery track. Searches the OpenLibrary API for
a book title and saves the results, this time practicing what happens when
the pipeline actually depends on an external service instead of just made-up
data.

## What it does

- `book_search` hits OpenLibrary's search API for a book title (comes from a
  DAG param, defaults to "Atomic Habits"), paging through results if there's
  more than one page
- `filter_response` pulls out just the fields I care about (title, author,
  publish year, edition count)
- `save` writes the result to `data/books/<date>.json`

## Running it

```bash
docker compose up
```

Unpause `02_api_incremental_ingestion` in the UI, trigger it (can override
`book_title` from the trigger screen), check `data/books/`.

## What I was practicing

- calling a real external API from a task instead of generating fake data
- handling pagination since OpenLibrary caps a single response at 100 results
- `raise_for_status()` + retries/execution_timeout, so a bad response or a
  slow API fails the task properly instead of quietly returning garbage
- using `.get()` instead of direct dict indexing since not every result from
  the API has every field

## Known gaps

- the API itself has no date filtering, so this isn't "incremental" in the
  same sense as project 1 was — the output is just tagged by `ds` as a daily
  snapshot rather than actually pulling different data per day
- no schema validation on the API response beyond the `.get()` fallbacks