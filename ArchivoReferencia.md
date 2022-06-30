# Examen Final 


 Una vez configurado nuestro **_PIPILINE_** que copia todas las tablas a la ruta señalada se procede a crear un **Notebook de spark** con el cual realizamos los siguientes pasos.
 
 ## Lectura del archivo csv

``Read CSV
dfPathResultadoCsv = spark.read.load('abfss://capacitacion@sesacapacitacion.dfs.core.windows.net/synapse/workspaces/synapsecapacitacion/warehouse/raw/jcabrera/clientes_correos.csv', format='csv')``


### LECTURA DE ARCHIVO DENTRO DE LA CARPETA jcabrera todos con formato PARQUET

#### tabla Cliente
``pathCliente='abfss://capacitacion@sesacapacitacion.dfs.core.windows.net/synapse/workspaces/synapsecapacitacion/warehouse/raw/jcabrera/cliente_jcabrera.parquet'
dfCliente = spark.read.load(pathCliente, format='parquet')``

#### Tabla Factura
``pathFactura='abfss://capacitacion@sesacapacitacion.dfs.core.windows.net/synapse/workspaces/synapsecapacitacion/warehouse/raw/jcabrera/factura_jcabrera.parquet'
dfFactura = spark.read.load(pathFactura, format='parquet')``

#### Tabla Producto
``pathProducto='abfss://capacitacion@sesacapacitacion.dfs.core.windows.net/synapse/workspaces/synapsecapacitacion/warehouse/raw/jcabrera/producto_jcabrera.parquet'
dfProducto = spark.read.load(pathProducto, format='parquet')``

#### Tabla Factura Producto
``pathFacturaProducto='abfss://capacitacion@sesacapacitacion.dfs.core.windows.net/synapse/workspaces/synapsecapacitacion/warehouse/raw/jcabrera/facturaproducto_jcabrera.parquet'
dfFacturaProducto = spark.read.load(pathFacturaProducto, format='parquet')``


### CREANDO TABLAS TEMPORALES

``dfPathResultadoCsv.createOrReplaceTempView("tbl_correo")``

``dfCliente.createOrReplaceTempView("tbl_cliente")``

``dfFactura.createOrReplaceTempView("tbl_factura")``

``dfProducto.createOrReplaceTempView("tbl_producto")``

`` dfFacturaProducto.createOrReplaceTempView("tbl_facturaProducto")``

### Producto mas vendido


``dfConsulta=spark.sql("SELECT rowidproducto, count(rowidproducto) from tbl_facturaProducto group by rowidproducto order by count(rowidproducto) desc limit 1")``

``dfConsulta.createOrReplaceTempView("tbl_productoMax")``

`` dfConsulta.show() ``
 ### Fecha de ültima Compra y su correo 
``vSQL="""``
``SELECT d.rowidcliente as codigoCliente,  e._c1 as email,  c.producto producto, max(d.fecha) as fechaUltimaCompra from tbl_productoMax a``

``inner join tbl_facturaProducto b on a.rowidproducto = b.rowidproducto``

``inner join tbl_Producto c  on b.rowidproducto = c.rowidproducto``

``inner join tbl_factura d on b.rowidfactura = d.rowidfactura``

``inner join tbl_correo e on d.rowidcliente = e._c0``

``group by d.rowidcliente,c.producto, e._c1``

``"""

``dfResultadoSql=spark.sql(vSQL)``

``dfResultadoSql.show()``
 
### Cargando datos al Pool de SQL 
``dfResultadoSql.write.mode("overwrite").saveAsTable("default.tbl_jcabrera")``

``path='abfss://capacitacion@sesacapacitacion.dfs.core.windows.net/synapse/workspaces/synapsecapacitacion/warehouse/raw/jcabrera/tbl_jcabrera.parquet'``

``dfResultadoSql.repartition(1).write.mode("overwrite").parquet(path)``

``dfResultadoSql.show()``



