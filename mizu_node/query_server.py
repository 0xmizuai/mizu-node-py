from fastapi import FastAPI, HTTPException, Depends, Query as QueryParam
from fastapi.middleware.cors import CORSMiddleware
from typing import Annotated

from mizu_node.common import build_ok_response, error_handler
from contextlib import asynccontextmanager
import uvicorn
from mizu_node.db.query import (
    get_owned_queries,
    get_query_detail,
    get_query_results,
    save_new_query,
)
from mizu_node.types.connections import Connections
from mizu_node.types.query_service import (
    PaginatedQueryResults,
    QueryContext,
    QueryDetails,
    QueryList,
    QueryResult,
    RegisterQueryRequest,
    RegisterQueryResponse,
)
from mizu_node.db.orm.query import Query
from mizu_node.security import verify_internal_service


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.conn = Connections()
    yield


app = FastAPI(lifespan=lifespan)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    return {"status": "ok"}


@app.get("/health")
async def health_check():
    return {"status": "ok"}


@app.post("/register_query")
@error_handler
async def register_query(
    query: RegisterQueryRequest, _: Annotated[bool, Depends(verify_internal_service)]
):
    async with app.state.conn.get_query_db_session() as db:
        query_id = await save_new_query(
            db,
            dataset=query.dataset,
            language=query.language,
            query_text=query.query_text,
            model=query.model,
            user=query.user,
        )
        return build_ok_response(RegisterQueryResponse(query_id=query_id))


@app.get("/queries/{query_id}/results", response_model=PaginatedQueryResults)
@error_handler
async def get_query_results_endpoint(
    query_id: int,
    user: str,
    _: Annotated[bool, Depends(verify_internal_service)],
    page: int = QueryParam(default=1, ge=1),
):
    async with app.state.conn.get_query_db_session() as db:
        # Verify query belongs to publisher
        query = (
            db.query(Query)
            .filter(
                Query.id == query_id,
                Query.user == user,
                Query.status != "pending",
                Query.results.any(),
            )
            .first()
        )

        if not query:
            raise HTTPException(status_code=404, detail="Query not found")

        # Get paginated results
        results, total = await get_query_results(db, query_id, page)

        page_size = 1000
        return build_ok_response(
            PaginatedQueryResults(
                results=[
                    QueryResult(
                        results=r.results,
                    )
                    for r in results
                ],
                total=total,
                page=page,
                page_size=page_size,
                has_more=total > page * page_size,
            )
        )


@app.get("/queries/{query_id}", response_model=QueryContext)
async def get_query_context(
    query_id: int, _: Annotated[bool, Depends(verify_internal_service)]
):
    async with app.state.conn.get_query_db_session() as db:
        query = await get_query_detail(db, query_id)
        if not query:
            raise HTTPException(status_code=404, detail="Query not found")
        return build_ok_response(
            QueryContext(query_text=query.query_text, model=query.model)
        )


@app.get("/queries", response_model=QueryContext)
async def get_all_queries(
    user: str, _: Annotated[bool, Depends(verify_internal_service)]
):
    async with app.state.conn.get_query_db_session() as db:
        queries = await get_owned_queries(db, user=user)
        return build_ok_response(
            QueryList(
                queries=[
                    QueryDetails(
                        query_id=q.id,
                        dataset=q.dataset,
                        language=q.language,
                        query_text=q.query_text,
                        model=q.model,
                        created_at=q.created_at,
                    )
                    for q in queries
                ]
            )
        )


def start():
    """Start production server"""
    uvicorn.run(
        "mizu_node.query_server:app",
        host="0.0.0.0",
        port=8000,
        proxy_headers=True,
        forwarded_allow_ips="*",
    )


def start_dev():
    """Start development server with hot reload"""
    uvicorn.run(
        "mizu_node.query_server:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        reload_dirs=["app"],
    )
