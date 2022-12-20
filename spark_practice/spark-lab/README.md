Погдотовка и отправка spark приложения на "удаленный сервер":
1. Собрать jar(Maven->Lifecycle->package).
2. spark-submit --class org.example.App --packages ./spark-lab-1.0-SNAPSHOT.jar