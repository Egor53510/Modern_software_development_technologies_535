import uvicorn
import os

if __name__ == "__main__":
    # Устанавливаем переменные окружения для разработки
    os.environ["ENVIRONMENT"] = "development"
    
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,  # Автоматическая перезагрузка при изменении кода
        log_level="info"
    )