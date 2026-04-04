from fastapi import FastAPI, Depends, HTTPException, status
from google.cloud import bigquery
from pydantic import BaseModel

app = FastAPI()

PROJECT_ID = "dev-489415"
DATASET = "property_mgmt"

# ---------------------------------------------------------------------------
# Pydantic Models
# ---------------------------------------------------------------------------

class IncomeCreate(BaseModel):
    amount: float
    date: str
    description: str

class ExpenseCreate(BaseModel):
    amount: float
    date: str
    category: str
    vendor: str
    description: str

class IncomeUpdate(BaseModel):
    amount: float = None
    date: str = None
    description: str = None

class ExpenseUpdate(BaseModel):
    amount: float = None
    date: str = None
    category: str = None
    vendor: str = None
    description: str = None

# ---------------------------------------------------------------------------
# Dependency: BigQuery client
# ---------------------------------------------------------------------------

def get_bq_client():
    client = bigquery.Client()
    try:
        yield client
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------

@app.get("/properties")
def get_properties(bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns all properties in the database.
    """
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state,
            postal_code,
            property_type,
            tenant_name,
            monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        ORDER BY property_id
    """

    try:
        results = bq.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    properties = [dict(row) for row in results]
    return properties

@app.get("/properties/{property_id}")
def get_property_by_id(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns a single property by its ID.
    """
    query = f"""
        SELECT * FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = {property_id}
        LIMIT 1
    """

    try:
        results = list(bq.query(query).result())
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    if not results:
        raise HTTPException(status_code=404, detail="Property not found")
    
    return dict(results[0])

# ---------------------------------------------------------------------------
# Income
# ---------------------------------------------------------------------------
@app.get("/income/{property_id}")
def get_income(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns all income records for a specific property.
    We CAST the ID to a STRING so it doesn't get distorted by JSON/JavaScript.
    """
    query = f"""
        SELECT 
            CAST(income_id AS STRING) AS income_id,
            property_id,
            amount,
            date,
            description
        FROM `{PROJECT_ID}.{DATASET}.income`
        WHERE property_id = {property_id}
    """

    try:
        results = bq.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    return [dict(row) for row in results]


@app.post("/income/{property_id}", status_code=status.HTTP_201_CREATED)
def create_income(property_id: int, body: IncomeCreate, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Creates a new income record.
    Returns the newly created ID as a string.
    """
    # We generate the ID in a variable first so we can return it to you
    new_id_query = "SELECT ABS(FARM_FINGERPRINT(GENERATE_UUID())) as val"
    id_result = list(bq.query(new_id_query).result())
    generated_id = id_result[0].val

    query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.income` (income_id, property_id, amount, date, description)
        VALUES (
            {generated_id},
            {property_id},
            {body.amount},
            '{body.date}',
            '''{body.description}'''
        )
    """

    try:
        bq.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    return {
        "message": "Income record created",
        "income_id": str(generated_id)  # Return as string to avoid scientific notation
    }


# ---------------------------------------------------------------------------
# Expenses
# ---------------------------------------------------------------------------

@app.get("/expenses/{property_id}")
def get_expenses(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns all expense records for a specific property.
    Casting expense_id to STRING to prevent JSON scientific notation.
    """
    query = f"""
        SELECT 
            CAST(expense_id AS STRING) AS expense_id,
            property_id,
            amount,
            date,
            category,
            vendor,
            description
        FROM `{PROJECT_ID}.{DATASET}.expenses`
        WHERE property_id = {property_id}
    """

    try:
        results = bq.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    return [dict(row) for row in results]


@app.post("/expenses/{property_id}", status_code=status.HTTP_201_CREATED)
def create_expense(property_id: int, body: ExpenseCreate, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Creates a new expense record for a property.
    Note: 'date' must be in YYYY-MM-DD format.
    """
    query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.expenses` (expense_id, property_id, amount, date, category, vendor, description)
        VALUES (
            ABS(FARM_FINGERPRINT(GENERATE_UUID())),
            {property_id},
            {body.amount},
            '{body.date}',
            '{body.category}',
            '''{body.vendor}''',
            '''{body.description}'''
        )
    """

    try:
        bq.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create expense record: {str(e)}"
        )

    return {"message": "Expense record created"}


# ---------------------------------------------------------------------------
# Additional: Income PUT/DELETE
# ---------------------------------------------------------------------------

@app.put("/income/{income_id}")
def update_income(income_id: str, body: IncomeUpdate, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Additional Endpoint 1: Updates an existing income record by its ID.
    """
    updates = []
    if body.amount is not None: updates.append(f"amount = {body.amount}")
    if body.date is not None: updates.append(f"date = '{body.date}'")
    if body.description is not None: updates.append(f"description = '''{body.description}'''")

    if not updates:
        raise HTTPException(status_code=400, detail="No fields provided for update")

    query = f"""
        UPDATE `{PROJECT_ID}.{DATASET}.income`
        SET {', '.join(updates)}
        WHERE CAST(income_id AS STRING) = '{income_id}'
    """

    try:
        bq.query(query).result()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Update failed: {str(e)}")

    return {"message": "Income record updated"}


@app.delete("/income/{income_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_income(income_id: str, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Additional Endpoint 2: Deletes a specific income record.
    """
    query = f"DELETE FROM `{PROJECT_ID}.{DATASET}.income` WHERE CAST(income_id AS STRING) = '{income_id}'"

    try:
        bq.query(query).result()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Deletion failed: {str(e)}")

    return None


# ---------------------------------------------------------------------------
# Additional: Expense PUT/DELETE
# ---------------------------------------------------------------------------

@app.put("/expenses/{expense_id}")
def update_expense(expense_id: str, body: ExpenseUpdate, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Additional Endpoint 3: Updates an existing expense record by its ID.
    """
    updates = []
    if body.amount is not None: updates.append(f"amount = {body.amount}")
    if body.date is not None: updates.append(f"date = '{body.date}'")
    if body.category is not None: updates.append(f"category = '{body.category}'")
    if body.vendor is not None: updates.append(f"vendor = '''{body.vendor}'''")
    if body.description is not None: updates.append(f"description = '''{body.description}'''")

    if not updates:
        raise HTTPException(status_code=400, detail="No fields provided for update")

    query = f"""
        UPDATE `{PROJECT_ID}.{DATASET}.expenses`
        SET {', '.join(updates)}
        WHERE CAST(expense_id AS STRING) = '{expense_id}'
    """

    try:
        bq.query(query).result()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Update failed: {str(e)}")

    return {"message": "Expense record updated"}


@app.delete("/expenses/{expense_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_expense(expense_id: str, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Additional Endpoint 4: Deletes a specific expense record.
    """
    query = f"DELETE FROM `{PROJECT_ID}.{DATASET}.expenses` WHERE CAST(expense_id AS STRING) = '{expense_id}'"

    try:
        bq.query(query).result()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Deletion failed: {str(e)}")

    return None