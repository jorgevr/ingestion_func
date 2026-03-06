---
name: pytest-coverage
description: 'Run pytest tests with coverage, discover lines missing coverage, and increase coverage to 100%. Prioritizes testing against real environments before mocks.'
---

# Pytest Coverage — Complete Testing Guide

The goal is for tests to cover **all lines of code** with meaningful, realistic assertions. Coverage percentage is a floor, not a ceiling — tests must validate actual behavior, not just execute lines.

---

## API Testing Priority (STRICTLY FOLLOW THIS ORDER)

When writing tests that involve API or HTTP calls, always attempt to test in this order:

### 1. Production Environment (Highest Priority)
Test against the real production URL whenever it is safe to do so (read-only operations, non-destructive calls).

```python
def test_get_user_production():
    """Test against real production endpoint."""
    response = requests.get("https://api.yourservice.com/users/1")
    assert response.status_code == 200
    data = response.json()
    # Assert real structure, not assumed structure
    assert "id" in data
    assert "email" in data
    assert isinstance(data["id"], int)
```

**Why:** Real environments catch auth issues, schema drift, network edge cases, and contract regressions that mocks never will.

### 2. Stable Non-Production Environment
If production testing is unsafe (write operations, destructive calls, rate-limited endpoints), use staging/UAT.

```python
BASE_URL = os.environ.get("API_BASE_URL", "https://staging.yourservice.com")

def test_create_order_staging():
    """Test write operations against staging."""
    response = requests.post(f"{BASE_URL}/orders", json={"item": "test"})
    assert response.status_code in (200, 201)
```

### 3. Mock (Last Resort Only)
Use mocks **only** when hitting a real environment is impossible (third-party paid APIs, infrastructure not available in CI, destructive irreversible operations).

```python
# DOCUMENT WHY you're mocking — it should be an exception, not a default
def test_stripe_payment_mock():
    """
    Mocked: Stripe charges real money in production.
    Replace with staging key when Stripe test mode is available.
    """
    with patch("stripe.Charge.create") as mock_charge:
        mock_charge.return_value = {"id": "ch_test", "status": "succeeded"}
        result = process_payment(amount=1000)
        assert result["status"] == "succeeded"
```

---

## Generating Coverage Reports

### Full project coverage
```bash
pytest --cov --cov-report=annotate:cov_annotate
```

### Specific module
```bash
pytest --cov=your_module_name --cov-report=annotate:cov_annotate
```

### Specific test file targeting a module
```bash
pytest tests/test_your_module.py --cov=your_module_name --cov-report=annotate:cov_annotate
```

### Combined terminal + annotated output (recommended)
```bash
pytest --cov=your_module_name \
       --cov-report=annotate:cov_annotate \
       --cov-report=term-missing \
       -v
```

---

## Reading the Annotated Report

Open the `cov_annotate/` directory after running the report. Rules:

- **One file per source file** — named after the original source.
- **File at 100% coverage** → skip it, all lines are exercised.
- **Lines starting with `!`** → not covered by any test. These are your targets.
- **Lines starting with `>`** → covered and executed.

### Workflow
```
1. Run pytest with --cov-report=annotate:cov_annotate
2. List files in cov_annotate/ — note which are NOT at 100%
3. Open each under-covered file
4. Find every line starting with !
5. Understand WHY that line isn't hit (missing branch, untested error path, dead code)
6. Write a test that exercises that specific path
7. Re-run coverage
8. Repeat until all files are clean
```

---

## Best Practices

### Structure: AAA Pattern (Arrange, Act, Assert)
Every test must follow this structure explicitly:

```python
def test_order_total_with_discount():
    # Arrange
    cart = Cart(items=[Item("widget", price=100)])
    coupon = Coupon(code="SAVE10", discount=0.10)

    # Act
    total = cart.calculate_total(coupon=coupon)

    # Assert
    assert total == 90.0
```

### Analyze Real API Responses — Don't Assume
When testing against a real endpoint, **print and inspect** the actual response before writing assertions. Never assume the schema.

```python
def test_analyze_api_response():
    """Exploratory test — run once to understand real response shape."""
    response = requests.get("https://api.example.com/products/1")
    print("\n--- REAL RESPONSE ---")
    print(f"Status: {response.status_code}")
    print(f"Headers: {dict(response.headers)}")
    print(f"Body: {json.dumps(response.json(), indent=2)}")
    # After inspecting output, write precise assertions
    assert response.status_code == 200
```

### Assert Behavior, Not Just Execution
Bad (line coverage only):
```python
def test_bad():
    result = calculate_tax(100)
    assert result is not None  # Useless
```

Good (validates real behavior):
```python
def test_tax_calculation_us_standard_rate():
    result = calculate_tax(amount=100, region="US", state="CA")
    assert result == 8.25  # California tax rate
    assert isinstance(result, float)
```

### Test All Branches — Not Just the Happy Path
```python
# Cover every branch of this function:
# def get_discount(user_type): 
#     if user_type == "vip": return 0.20
#     elif user_type == "member": return 0.10
#     else: return 0.0

@pytest.mark.parametrize("user_type,expected_discount", [
    ("vip", 0.20),
    ("member", 0.10),
    ("guest", 0.0),
    ("unknown_type", 0.0),  # Edge case
    ("", 0.0),              # Empty string
    (None, 0.0),            # None guard
])
def test_discount_all_branches(user_type, expected_discount):
    assert get_discount(user_type) == expected_discount
```

### Test Error Paths and Exception Messages
```python
def test_api_raises_on_unauthorized():
    with pytest.raises(PermissionError, match="401 Unauthorized"):
        fetch_protected_resource(token="invalid_token")

def test_api_raises_on_timeout():
    with pytest.raises(requests.Timeout):
        with patch("requests.get", side_effect=requests.Timeout):
            fetch_data_with_real_timeout_scenario()
```

### Use Fixtures for Shared Setup — Never Repeat Yourself
```python
# conftest.py
@pytest.fixture(scope="session")
def api_base_url():
    """Single source of truth for environment targeting."""
    return os.environ.get("API_BASE_URL", "https://api.yourservice.com")

@pytest.fixture
def authenticated_session(api_base_url):
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {os.environ['API_TOKEN']}",
        "Content-Type": "application/json"
    })
    return session

def test_get_profile(authenticated_session, api_base_url):
    response = authenticated_session.get(f"{api_base_url}/profile")
    assert response.status_code == 200
```

### Always Test Response Structure for API Calls
```python
def test_api_response_contract(authenticated_session, api_base_url):
    """Validate the API contract — catches schema changes early."""
    response = authenticated_session.get(f"{api_base_url}/users/1")
    
    assert response.status_code == 200
    assert response.headers["Content-Type"] == "application/json"
    
    data = response.json()
    
    # Assert required fields exist
    required_fields = {"id", "email", "name", "created_at"}
    assert required_fields.issubset(data.keys()), \
        f"Missing fields: {required_fields - data.keys()}"
    
    # Assert field types
    assert isinstance(data["id"], int)
    assert isinstance(data["email"], str)
    assert "@" in data["email"]
```

### Use Environment Variables for Secrets — Never Hardcode
```python
# BAD
token = "sk-prod-abc123secret"

# GOOD
token = os.environ.get("API_TOKEN")
if not token:
    pytest.skip("API_TOKEN not set — skipping live API test")
```

### Mark Tests by Category
```python
@pytest.mark.live       # Hits real network
@pytest.mark.slow       # Takes >1s
@pytest.mark.destructive  # Writes/deletes data
@pytest.mark.integration
@pytest.mark.unit

# Run only fast unit tests in CI pre-commit
# pytest -m "unit and not slow"

# Run full suite including live tests nightly
# pytest -m "live or integration"
```

Configure markers in `pytest.ini`:
```ini
[pytest]
markers =
    live: tests that call real external APIs
    slow: tests that take more than 1 second
    destructive: tests that create or delete real data
    integration: integration tests
    unit: unit tests
addopts = -v --tb=short
```

### Fixture Scopes — Use the Right One
```python
@pytest.fixture(scope="session")   # Once per test run — expensive setup (DB connections, auth tokens)
@pytest.fixture(scope="module")    # Once per file — shared client instances
@pytest.fixture(scope="function")  # Default — fresh state per test (safest)
```

---

## Iterative Coverage Loop

Follow this loop until `cov_annotate/` shows no `!` lines:

```bash
# Step 1: Baseline
pytest --cov=your_module --cov-report=annotate:cov_annotate --cov-report=term-missing -v

# Step 2: Check which files are below 100%
# (term-missing shows exact line numbers in terminal)

# Step 3: Open annotated file, find ! lines, write tests

# Step 4: Re-run
pytest --cov=your_module --cov-report=annotate:cov_annotate --cov-report=term-missing -v

# Step 5: Repeat until output shows:
# TOTAL    xxx    0   100%
```

---

## Configuration Reference

`pytest.ini`:
```ini
[pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts =
    -v
    --strict-markers
    --tb=short
markers =
    live: hits real external APIs
    slow: takes more than 1 second
    destructive: writes or deletes real data
    integration: integration tests
    unit: pure unit tests
```

`pyproject.toml` equivalent:
```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = ["-v", "--tb=short", "--strict-markers"]

[tool.coverage.run]
source = ["your_module"]
omit = ["*/tests/*", "*/migrations/*", "*/__init__.py"]

[tool.coverage.report]
fail_under = 100
show_missing = true
exclude_lines = [
    "pragma: no cover",
    "def __repr__",
    "raise NotImplementedError",
    "if TYPE_CHECKING:",
]
```

---

## Quick Reference Checklist

Before marking a module as fully tested:

- [ ] All lines covered (`!` lines eliminated from `cov_annotate/`)
- [ ] Happy path tested against real/staging URL where possible
- [ ] All error branches tested (4xx, 5xx, timeouts, invalid input)
- [ ] Response schema/contract validated for all API calls
- [ ] Parametrized tests cover edge cases (empty, None, negative, boundary values)
- [ ] Secrets come from environment variables, never hardcoded
- [ ] Tests are independent — no shared mutable state between tests
- [ ] Fixtures handle setup/teardown, not manual code in each test
- [ ] Mocks are documented with a reason when used instead of real endpoints
