# Learning Log — Crypto ETL Pipeline
> A personal journal of lessons learned while building 
> a production-style ETL pipeline.
> Each lesson includes: what went wrong, why it matters, 
> and the correct pattern.

---

# 📚 LESSONS

# Session 1 — April 9, 2026

### Lesson 1: Never use bare `except:`
**What I had:**
```python
except:
    logger.info("Can't fetch rate")
```
**Why it's dangerous:** Catches everything including memory errors and
keyboard interrupts. You can never know what actually failed.

**What I learned:**
```python
# Use this when you need to log the full traceback:
except requests.exceptions.RequestException:
    logger.error("Failed to fetch rate", exc_info=True)

# Use this when you want to include the error in a custom message:
except requests.exceptions.RequestException as e:
    logger.error(f"Failed to fetch rate: {e}")
```
**Rule to remember:** Always catch the most specific exception possible.
`exc_info=True` prints the full stack trace automatically.

---

### Lesson 2: Never commit `.env` or hardcode credentials
**What happened:** Committed `.env` by mistake and hardcoded 
passwords in `config.py`.

**Why it's dangerous:** Your credentials are visible to everyone
who opens your repo. Bots scan GitHub 24/7 for this.

**What I learned:**
```bash
# Step 1 — Check if .env was ever committed
git log --all --full-history -- .env

# Step 2 — Remove from tracking
git rm --cached .env

# Step 3 — Clean full history (use git-filter-repo, not filter-branch)
pip install git-filter-repo
git filter-repo --path .env --invert-paths --force
git remote add origin <your-repo-url>
git push origin --force --all
```
**Rule to remember:** Add `.env` to `.gitignore` BEFORE your 
first commit. Use `.env.example` to document required variables.

---

### Lesson 3: The `main()` pattern
**Why it matters:** Code at module level runs even after a failure.
Wrapping in `main()` lets you use `return` to exit cleanly.
It also makes your script importable without executing it.

**Before — dangerous:**
```python
# If this fails, the code below still runs and crashes with NameError
try:
    rate = get_rate(api_url)
except requests.exceptions.RequestException:
    logger.error("Failed", exc_info=True)
    spark.stop()

enriched_df = flattened_df.withColumn(...)  # ← crashes here
```

**After — professional:**
```python
def main():
    try:
        rate = get_rate(api_url)
    except requests.exceptions.RequestException:
        logger.error("Failed to fetch rate", exc_info=True)
        spark.stop()
        return  # ← exits cleanly, nothing else runs

if __name__ == "__main__":
    main()
```

---

### Lesson 4: `os.getenv()` — two patterns
```python
# Secrets — no default, fails loudly if missing:
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Non-secrets — safe default if .env not present:
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://master:7077")
```
**Rule to remember:** If a secret is missing, the app should 
crash immediately — not connect silently with wrong credentials.

---

### Lesson 5: Avoid recomputation in Spark
**Why it matters:** Every `count()` triggers a full data scan.
```python
# ❌ Scans data 3 times
if df.count() > 0:
    save(df)
    logger.info(f"{df.count()} rows saved")

# ✅ Scans data once
row_count = df.count()
if row_count > 0:
    save(df)
    logger.info(f"{row_count} rows saved")
```

---


# 🔮 SESSION 2 — April 10, 2026

### Lesson 7: PEP8 Import Ordering
**Rule:** Always order imports in 3 groups:
1. Standard library (os, json, logging, datetime)
2. Third party (requests, boto3, pyspark, airflow)
3. Local imports (from utils import ...)

Within each group — alphabetical order.
Every professional Python linter enforces this automatically.

---

### Lesson 8: os.makedirs — always create directory before writing
**Problem:** open(file_path, "a") crashes if directory doesn't exist.
**Fix:**
```python
os.makedirs(os.path.dirname(file_path), exist_ok=True)
```
`exist_ok=True` means: create if missing, do nothing if exists.
**Rule:** Always call this before writing any file. Never assume
the directory exists.

---

### Lesson 9: Fail Slow Validation Pattern
**Problem:** Returning on first failure hides other problems.
```python
# ❌ Fail fast — stops at first error, hides the rest
for column in columns:
    if has_nulls(column):
        return False   # columns 2 and 3 never checked

# ✅ Fail slow — checks everything, reports all problems
is_valid = True
for column in columns:
    if has_nulls(column):
        logger.warning(f"Column {column} has nulls")
        is_valid = False   # continue checking
return is_valid
```
**Rule:** In data validation, always check ALL conditions
before returning. You want to see every problem at once,
not discover them one by one across multiple pipeline runs.

**Real tool that uses this pattern:** Great Expectations
→ research this library, it's the industry standard for
data quality in Python pipelines.

---

### Lesson 10: DRY — Don't Repeat Yourself
**Problem:** Same function copy-pasted in 3 DAG files.
**Fix:** Move shared functions to a utils module and import.

**In our project:**
- upload_to_minio() → moved to dags/utils.py
- append_json_line() → moved to dags/utils.py
- get_logger() → moved to dags/utils.py

**Rule:** If you write the same code twice — it belongs
in a shared module. The third time you copy it,
you've already made a mistake.

---

### Lesson 11: Singleton Pattern
**What it is:** Create an expensive object ONCE, reuse it
instead of recreating on every function call.

```python
# ❌ Creates new connection every call — wasteful
def upload_to_minio(file_path, bucket, object_name):
    client = boto3.client('s3', ...)  # new connection every time
    client.upload_file(...)

# ✅ Singleton — creates connection only first time
_s3_client = None

def get_s3_client():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client('s3', ...)
    return _s3_client

def upload_to_minio(file_path, bucket, object_name):
    client = get_s3_client()  # reuses existing connection
    client.upload_file(...)
```
**When it matters:** High frequency calls (every second/minute).
For hourly pipelines — acceptable to skip.
**Common candidates:** Database connections, API clients,
Spark sessions, boto3 clients.

---

# 🗂️ GIT REFERENCE

## Conventional Commits
```bash
feat:      new feature
fix:       bug fix
refactor:  code change that isn't a fix or feature
docs:      documentation only
chore:     maintenance tasks
security:  security fix

# Example:
git commit -m "feat: add data validation module"
```

## Branch Strategy

main  → production only, never commit directly here
dev   → daily work, merge to main when feature is complete

## Useful Commands:
```bash
# History
git log --oneline -10
git log --oneline --graph --decorate -10
git show HEAD

# Branches
git checkout -b new-branch        # create and switch
git branch -d branch_name         # delete locally
git push origin --delete name     # delete from GitHub

# Stash (save work temporarily)
git stash push -u -m "description"
git stash pop                      # restore and delete stash
git stash apply                    # restore but keep stash

# Copy file from another branch
git restore --source=branch_name path/to/file

# Fix upstream tracking
git push --set-upstream origin dev

# Remove file from git tracking
git rm --cached filename
```

---


