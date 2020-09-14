## Main features

### FacebookExecutor

Python utility to perform task against the Facebook API

#### Example

```python
from pygyver.etl.facebook import FacebookExecutor

# Login
fb = FacebookExecutor()

# Get Active Campaign budget for the Facebook account 12346789
account_id = '12346789'
active_campaigns = fbd.get_active_campaign_budgets(
    account_id
)
```
