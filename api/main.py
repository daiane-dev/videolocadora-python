from fastapi import FastAPI

from api.routes.filmes import router as filmes_router
from api.routes.usuarios import router as usuarios_router
from api.routes.locacoes import router as locacoes_router
from api.routes.dashboard import router as dashboard_router
from api.routes.views import router as views_router

app = FastAPI(title="Videolocadora API", version="0.1.0")

app.include_router(filmes_router, tags=["Filmes"])
app.include_router(usuarios_router, tags=["Usuários"])
app.include_router(locacoes_router, tags=["Locações"])
app.include_router(dashboard_router, tags=["Dashboard"])
app.include_router(views_router, tags=["Views"])


@app.get("/health")
def health():
    return {"status": "ok"}    
