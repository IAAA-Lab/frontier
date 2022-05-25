# Frontier

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=IAAA-Lab_frontier&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=IAAA-Lab_frontier)

El objetivo de **Frontier** es el desarrollo de una *frontera de rastreo* en Kotlin basada en el estándar industrial
[URL Frontier](https://github.com/crawler-commons/url-frontier) que permite exponer su interfaz para ser utilizado 
por *rastreadores web*. Al implementar este estándar, **Frontier** resuelve los siguientes problemas:

- Uso concurrente de una frontera de rastreo por diversos rastreadores web ya que funciona de una forma similar a un 
  sistema de tenencia múltiple, en la cual hay una única instancia de la frontera de rastreo pero sirviendo a múltiples 
  procesos.
- Uso distribuido de una frontera de rastreo, ya que al estar URL Frontier especificado en gRPC puede ser utilizado por 
  múltiples rastreadores web remotos.
- La falta de una implementación en Kotlin de dicho estándar. Kotlin, lenguaje de la familia Java elegido por Google 
  como el lenguaje oficial para la programación de aplicaciones Android, está siendo utilizado de forma cada vez más 
  amplia por la industria del desarrollo de software en apliaciones de backend. 

Un rastreador web, a veces llamado araña web, es un sistema que navega sistemáticamente por la Web, y que suele ser 
operado con el propósito de indexar la Web con diversos propósitos tanto comerciales como científicos. 
La frontera de rastreo es uno de los componentes que conforman la arquitectura de un rastreador web.
La frontera de rastreo contiene la lógica y las políticas que sigue un rastreador cuando visita sitios web. 
Las políticas pueden incluir aspectos como qué páginas deben ser visitadas a continuación, las prioridades para cada 
página a buscar, y la frecuencia con la que la página debe ser visitada. La eficiencia de la frontera de rastreo es 
un aspecto especialmente importante de su diseño ya que una de las características de la Web que hacen que el rastreo 
de la web sea un desafío por contener un gran volumen de datos y está en constante cambio.

El estándar industrial URL Frontier ha definido un API con utilizando el protocolo gRPC para las operaciones de las 
fronteras de rastreo. Este estándar ha sido financiado por el fondo NGI0 Discovery Fund, un fondo apoyado por 
el programa Next Generation Internet de la Comisión Europea. 
