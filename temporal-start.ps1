# Temporal dev server: in-memory persistence + Web UI at http://localhost:8233.
# --search-attribute pre-registers the OrderStatus keyword attribute so the first
# workflow task does not fail with BadSearchAttributes.
temporal server start-dev `
    --search-attribute "OrderStatus=Keyword"
