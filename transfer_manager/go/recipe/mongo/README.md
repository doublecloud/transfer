# Рецепт для шардированной MongoDB 4.x

В основе данного рецепта лежит туториал по развёртке
шардированного кластера: https://www.mongodb.com/docs/manual/tutorial/deploy-shard-cluster/
По сути, рецепт автоматизирует этот процесс и в зависимости
от описания шардов предоставляет вам готовый кластер, к которому
можно подключиться.

Пока что поддерживаются тольо версии 4.0, 4.2, 4.4.

Проблема с поддержкой версии 5.0+: DEVTOOLSSUPPORT-21944

## Структура проекта
* `example` -- примеры
  * `recipe_usage` -- пример использования рецепта
  * `configs` -- примеры конфигураций шардированных кластеров
  * `launch_cluster` -- пример запуск кластера напрямую из Go кода, а
                      не из рецепта
* `pkg` -- рецепт написан на языке Go, поэтому здесь содержится
           код запуска рецепта
  * `cluster` -- инфраструктурный код запуска шардированного кластера
  * `config` -- код для обработки конфигурации кластера
  * `binurl` -- вспомогательный код парсинга странички с
              бинарями MongoDB
  * `tar` -- код для работы с `tar` архивами в рецепте и в упаковщике
             бинарей MongoDB в единый архив
  * `test` -- проверка работоспособности рецепта. На текущий момент
              поддерживаются только версии 4.0, 4.2, 4.4. На данный момент
              поддержка 5.0+ невозможно, смотри тикет
              [DEVTOOLSSUPPORT-21944](https://st.yandex-team.ru/DEVTOOLSSUPPORT-21944)
* `cmd` -- additional utility
    * `binurl` -- utility that helps pack official MongoDB distribution
      with a variety of version for specific platform and OS into
      single archive, that can be uploaded into Sandbox.
    * `launch_cluster` -- just example how you can deploy your cluster
      from code
