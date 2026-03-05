import datetime as dt
from sqlalchemy import Column, Integer, String, DateTime, Float
from .models import Base

__all__ = [
    "ForecastBase",
    "ForecastValorizado",
    "ForecastArticulo",
    "ForecastNegocio",
    "ForecastCliente",
    "ForecastDatasetBase"
]

class ForecastBase(Base):
    __tablename__ = "forecast_base"
    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    fecha = Column(DateTime, index=True)
    periodo = Column(String(20), index=True)
    codigo_serie = Column(String(100), index=True)
    nivel_agregacion = Column(String(50))
    perfil = Column(String(50), index=True)
    neg = Column(Integer, index=True)
    subneg = Column(Integer, index=True)
    familia = Column(String(200))
    tipo = Column(String(50), index=True)
    y = Column(Float)
    yhat = Column(Float)
    li = Column(Float)
    ls = Column(Float)
    submodelo = Column(String(50))
    clasificacion_serie = Column(String(50))
    version_param = Column(String(50))
    precio = Column(Float)
    etiqueta_upper = Column(String(50))

class ForecastValorizado(Base):
    __tablename__ = "forecast_valorizado"
    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    fecha = Column(DateTime, index=True)
    periodo = Column(String(20), index=True)
    codigo_serie = Column(String(100), index=True)
    perfil = Column(String(50), index=True)
    cliente_id = Column(String(50), index=True)
    grupo = Column(String(200), index=True)
    periodo_ponderacion = Column(String(20))
    ponderador_mes = Column(Float)
    yhat_cliente = Column(Float)
    li_cliente = Column(Float)
    ls_cliente = Column(Float)
    clasificacion_serie = Column(String(50))
    nivel_agregacion = Column(String(50))
    precio_base = Column(Float)
    precio_ajustado = Column(Float)
    monto_yhat = Column(Float)
    monto_li = Column(Float)
    monto_ls = Column(Float)
    months_diff = Column(Integer)
    submodelo = Column(String(50))
    version_param = Column(String(50))

class ForecastArticulo(Base):
    __tablename__ = "forecast_articulo"
    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    codigo = Column(String(100), index=True)
    descrip = Column(String(300))
    predrog = Column(String(200))
    cantenv = Column(Float)
    laboratorio_descrip = Column(String(200), index=True)
    familia = Column(String(200))
    unineg = Column(String(100))
    sunineg = Column(String(100))

class ForecastNegocio(Base):
    __tablename__ = "forecast_negocio"
    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    unidad = Column(Integer, index=True)
    subunidad = Column(Integer, index=True)
    descrip = Column(String(200))

class ForecastCliente(Base):
    __tablename__ = "forecast_cliente"
    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    codigo = Column(String(50), index=True)
    nombre = Column(String(200))
    fantasia = Column(String(200))
    grupo = Column(String(50))
    perfil = Column(String(100), index=True)
    provincia = Column(String(100))
    vendedor_abrev = Column(String(100))
    cliente_grupo = Column(String(50), index=True)
    nombre_grupo = Column(String(200), index=True)
    tipocli = Column(String(50))

class ForecastDatasetBase(Base):
    __tablename__ = "forecast_dataset_base"
    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    codigo_serie = Column(String(100), index=True)
    perfil = Column(String(50), index=True)
    qty_mes = Column(Float)
    periodo = Column(String(20), index=True)
    nivel_agregacion = Column(String(50))
    neg = Column(Integer, index=True)
    subneg = Column(Integer, index=True)
    familia = Column(String(200))
    fecha = Column(DateTime, index=True)
