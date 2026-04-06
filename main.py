from fastapi import FastAPI, Depends, HTTPException, status
from google.cloud import bigquery
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

PROJECT_ID = "dev-489415"
DATASET = "property_mgmt"

# CORS middleware tells the browser which cross-origin requests are allowed.
# Allowing all origins ("*") is fine for a classroom demo but should be
# restricted to specific domains in a real production application.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],       # accept requests from any origin
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],       # accept any request headers
)


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
    check_query = f"SELECT property_id FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id = {property_id} LIMIT 1"
    
    try:
        prop_check = list(bq.query(check_query).result())
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Property check failed: {str(e)}")

    if not prop_check:
        raise HTTPException(status_code=404, detail=f"Property with ID {property_id} not found")
    
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
    Creates a new income record, but only if the property exists.
    """
    
    # 1. VALIDATION: Check if the property exists first
    check_query = f"""
        SELECT property_id 
        FROM `{PROJECT_ID}.{DATASET}.properties` 
        WHERE property_id = {property_id} 
        LIMIT 1
    """
    
    try:
        prop_check = list(bq.query(check_query).result())
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error verifying property: {str(e)}"
        )

    if not prop_check:
        # If no property is found, stop here and return an error
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail=f"Cannot create income: Property ID {property_id} does not exist."
        )

    # 2. GENERATE ID: Now that we know the property is valid, get a new ID
    new_id_query = "SELECT ABS(FARM_FINGERPRINT(GENERATE_UUID())) as val"
    id_result = list(bq.query(new_id_query).result())
    generated_id = id_result[0].val

    # 3. INSERT: Create the record
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
            detail=f"Database insertion failed: {str(e)}"
        )

    return {
        "message": "Income record created",
        "income_id": str(generated_id)
    }


# ---------------------------------------------------------------------------
# Expenses
# ---------------------------------------------------------------------------

@app.get("/expenses/{property_id}")
def get_expenses(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns all expense records for a specific property.
    Validates property existence first.
    """
    # 1. Check if the property exists
    check_query = f"SELECT property_id FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id = {property_id} LIMIT 1"
    
    try:
        prop_check = list(bq.query(check_query).result())
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Property check failed: {str(e)}")

    if not prop_check:
        raise HTTPException(status_code=404, detail=f"Property with ID {property_id} not found")

    # 2. Fetch the expenses
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
    Creates a new expense record for a property only if it exists.
    Returns the newly created ID.
    """
    # 1. VALIDATION: Check if the property exists
    check_query = f"SELECT property_id FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id = {property_id} LIMIT 1"
    
    try:
        prop_check = list(bq.query(check_query).result())
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error verifying property: {str(e)}")

    if not prop_check:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail=f"Cannot create expense: Property ID {property_id} does not exist."
        )

    # 2. GENERATE ID: Same logic as income for consistency
    new_id_query = "SELECT ABS(FARM_FINGERPRINT(GENERATE_UUID())) as val"
    id_result = list(bq.query(new_id_query).result())
    generated_id = id_result[0].val

    # 3. INSERT: Create the record
    query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.expenses` (expense_id, property_id, amount, date, category, vendor, description)
        VALUES (
            {generated_id},
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

    return {
        "message": "Expense record created",
        "expense_id": str(generated_id)
    }

# ---------------------------------------------------------------------------
# Additional: Income PUT/DELETE
# ---------------------------------------------------------------------------

@app.put("/income/{income_id}")
def update_income(income_id: str, body: IncomeUpdate, bq: bigquery.Client = Depends(get_bq_client)):
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
        query_job = bq.query(query)
        query_job.result() # Wait for job to finish
        
        # Check if any row was actually updated
        if query_job.num_dml_affected_rows == 0:
            raise HTTPException(status_code=404, detail=f"Income record {income_id} not found")
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Update failed: {str(e)}")

    return {"message": "Income record updated"}


@app.delete("/income/{income_id}")
def delete_income(income_id: str, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"DELETE FROM `{PROJECT_ID}.{DATASET}.income` WHERE CAST(income_id AS STRING) = '{income_id}'"

    try:
        query_job = bq.query(query)
        query_job.result()

        if query_job.num_dml_affected_rows == 0:
            raise HTTPException(status_code=404, detail=f"Income record {income_id} not found")
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Deletion failed: {str(e)}")

    # We return a message here instead of 204 because 204 hides the response body 
    # which makes debugging the "not found" logic harder in some tools.
    return {"message": "Income record deleted"}

# ---------------------------------------------------------------------------
# Additional: Expense PUT/DELETE
# ---------------------------------------------------------------------------

@app.put("/expenses/{expense_id}")
def update_expense(expense_id: str, body: ExpenseUpdate, bq: bigquery.Client = Depends(get_bq_client)):
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
        query_job = bq.query(query)
        query_job.result()

        if query_job.num_dml_affected_rows == 0:
            raise HTTPException(status_code=404, detail=f"Expense record {expense_id} not found")
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Update failed: {str(e)}")

    return {"message": "Expense record updated"}


@app.delete("/expenses/{expense_id}")
def delete_expense(expense_id: str, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"DELETE FROM `{PROJECT_ID}.{DATASET}.expenses` WHERE CAST(expense_id AS STRING) = '{expense_id}'"

    try:
        query_job = bq.query(query)
        query_job.result()

        if query_job.num_dml_affected_rows == 0:
            raise HTTPException(status_code=404, detail=f"Expense record {expense_id} not found")
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Deletion failed: {str(e)}")

    return {"message": "Expense record deleted"}
