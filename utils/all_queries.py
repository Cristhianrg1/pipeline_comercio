queries = {
    "pedidos_ultimos_6_meses": """
        SELECT
            FORMAT_DATE('%Y-%m', fechaPedido) AS month_year,
            COUNT(*) AS total_orders
        FROM prueba_Cristhian.pedidos
        GROUP BY month_year
        ORDER BY month_year DESC
        LIMIT 6;
    """,
    "ventas_por_categoria": """
        SELECT
            c.nombreCategoria AS category,
            SUM(CAST(pd.cantidad AS FLOAT64) * CAST(pd.precioUnitario AS FLOAT64)) AS total_sales
        FROM `prueba_Cristhian.pedidos_detalles` pd
        JOIN `prueba_Cristhian.productos` p ON REPLACE(pd.idProducto, ' ', '') = REPLACE(p.idProducto, ' ', '')
        JOIN `prueba_Cristhian.categorias` c ON REPLACE(p.idCategoria, ' ', '') = REPLACE(c.idCategoria, ' ', '')
        GROUP BY category
        ORDER BY total_sales DESC;
    """,
    "clientes_comprando_tofu": """
        SELECT
            DISTINCT cl.nombreEmpresa AS Customer
        FROM `prueba_Cristhian.pedidos_detalles` pd
        JOIN `prueba_Cristhian.productos` p ON REPLACE(CAST(pd.idProducto AS STRING), ' ', '') = REPLACE(CAST(p.idProducto AS STRING), ' ', '')
        JOIN `prueba_Cristhian.pedidos` pe ON REPLACE(CAST(pd.idPedido AS STRING), ' ', '') = REPLACE(CAST(pe.idPedido AS STRING), ' ', '')
        JOIN `prueba_Cristhian.clientes` cl ON REPLACE(CAST(pe.idCliente AS STRING), ' ', '') = REPLACE(CAST(cl.idCliente AS STRING), ' ', '')
        WHERE p.nombreProducto = 'Tofu';
    """,
    "transportadores_de_beverages": """
        SELECT
            t.nombreEmpresa AS Transporter,
            COUNT(*) AS deliveries
        FROM `prueba_Cristhian.pedidos` p
        JOIN `prueba_Cristhian.transportistas` t ON REPLACE(CAST(p.idTransportista AS STRING), ' ', '') = REPLACE(CAST(t.idTransportista AS STRING), ' ', '')
        JOIN `prueba_Cristhian.pedidos_detalles` pd ON REPLACE(CAST(p.idPedido AS STRING), ' ', '') = REPLACE(CAST(pd.idPedido AS STRING), ' ', '')
        JOIN `prueba_Cristhian.productos` pr ON REPLACE(CAST(pd.idProducto AS STRING), ' ', '') = REPLACE(CAST(pr.idProducto AS STRING), ' ', '')
        JOIN `prueba_Cristhian.categorias` c ON REPLACE(CAST(pr.idCategoria AS STRING), ' ', '') = REPLACE(CAST(c.idCategoria AS STRING), ' ', '')
        WHERE c.nombreCategoria = 'Beverages'
        GROUP BY Transporter
        ORDER BY deliveries DESC;
    """,
}
