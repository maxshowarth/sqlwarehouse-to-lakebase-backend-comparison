# Data Models

This directory contains all the data models for the application, including filters and response models.

## Structure

### Filter Models (`data_filters.py`)
- **CustomerFilters** - Customer data filtering
- **OrderFilters** - Order data filtering
- **OrderItemsFilters** - Order item data filtering
- **ProductFilters** - Product data filtering
- **StoreFilters** - Store data filtering
- **InventoryFilters** - Inventory snapshot filtering
- **PromotionFilters** - Promotion data filtering

### Response Models
- **CustomerResponse** (`customers.py`) - Customer data structure
- **ProductResponse** (`products.py`) - Product data structure
- **StoreResponse** (`stores.py`) - Store data structure
- **OrderResponse** (`orders.py`) - Order data structure
- **OrderItemResponse** (`order_items.py`) - Order item data structure
- **InventoryResponse** (`inventory.py`) - Inventory snapshot structure
- **PromotionResponse** (`promotions.py`) - Promotion data structure

### List Response Models (`list_response.py`)
- **StringList** - Generic container for lists of unique string values (categories, regions, segments, etc.)
- **IntList** - Generic container for lists of unique integer values
- **DateTimeList** - Generic container for lists of unique datetime values
- **DateBounds** - Date range data (start/end timestamps)

## Usage

### Importing Models
```python
from app.data.models import (
    CustomerFilters,
    CustomerResponse,
    ProductFilters,
    ProductResponse,
    # ... etc
)
```

### Protocol Return Types
The `DataAccess` protocol now uses Union types to allow both DataFrames and structured responses:

```python
def get_customers(self, filters: CustomerFilters) -> Union[pd.DataFrame, List[CustomerResponse]]:
    """Get customers based on filters."""
    ...

def list_store_cities(self) -> Union[pd.DataFrame, StringList]:
    """List all store cities."""
    ...
```

### Benefits
1. **Type Safety** - Clear contract about expected data structure
2. **Flexibility** - Backends can return either DataFrames or structured objects
3. **Documentation** - Self-documenting API with field descriptions
4. **Validation** - Pydantic validation for structured responses
5. **Future-Proof** - Easy to evolve from DataFrames to structured responses
6. **Simplified List Models** - Generic containers for dropdown data instead of unique classes

### Field Validation
All response models include:
- Proper field types (int, str, datetime, float, etc.)
- Literal types for constrained values (e.g., payment types, regions)
- Field descriptions for documentation
- Required vs optional fields as appropriate

### CSV Mapping
Each response model exactly mirrors the corresponding CSV structure:
- **customers.csv** → `CustomerResponse`
- **products.csv** → `ProductResponse`
- **stores.csv** → `StoreResponse`
- **orders.csv** → `OrderResponse`
- **order_items.csv** → `OrderItemResponse`
- **inventory_snapshots.csv** → `InventoryResponse`
- **promotions.csv** → `PromotionResponse`

### List Response Models
The list response models are designed to be generic containers for unique values used in dropdowns:
- **StringList** - For categories, regions, cities, segments, payment types, etc.
- **IntList** - For IDs, quantities, etc.
- **DateTimeList** - For timestamp lists
- **DateBounds** - For start/end date ranges

This approach eliminates the need for unique classes for each list type while maintaining type safety and clarity.
