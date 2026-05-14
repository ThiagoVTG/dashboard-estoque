@echo off
echo Atualizando Dashboard de Pedidos e Estoque...
echo.
python "C:\Users\thiag\Documents\Claude\BI estoque\processar_dashboard.py"
echo.
echo Abrindo dashboard...
start "" "C:\Users\thiag\Documents\Claude\BI estoque\DASHBOARD_PEDIDOS_ESTOQUE.html"
pause
