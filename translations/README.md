# Download and upload changes to Tanker

To interact with [tanker](https://tanker.yandex-team.ru/projects)  we use [i18n-sync package](https://a.yandex-team.ru/arcadia/data-ui/i18n-sync). The installation is supposed to be in this directory. Please read the instruction about [i18n process](https://a.yandex-team.ru/svn/trunk/arcadia/data-ui/i18n-sync/docs/autotickets.md).

## Do not touch in Tanker master branch. It is readonly.

## Short instructions:

### Installation and setup:

1. Install Node.js version 14 (inportant - 14), npm will install alongside:

   * **Linux** https://github.com/nodesource/distributions/blob/master/README.md

   * **macOS**
   ```bash
   brew install node@14
   ```

1. Install the `i18n-sync` package:

    ```bash
    npm i --save-dev @yandex-data-ui/i18n-sync
    ```

1. Place the `I18N_OAUTH=...` token for tanker access into the `.env` file. You can get it here: <https://nda.ya.ru/t/5NokqGwN4vkJPj>. Make sure you have enough rights to commit changes to the project.

1. Download the texts from the tanker:

    ```bash
    npx i18n-sync import
    ```

### Usage

Tanker is built on top of the git repository, meaning it has branches. Changes are automatically uploaded to the tanker branch corresponding to the current arcadia branch. Before uploading changes to tanker, they must be added to the local arcadia branch. Note that English and Russian versions must be specified for each of the keys.

This is the algorithm for working with texts:

1. First, you need to create a snapshot of the current texts, better do it locally
(`--local`):

    ```bash
    npx i18n-sync push --local
    ```

1. Then you can make changes to the texts and text statuses. Afterwards, you can see the changes diff:

    ```bash
    npx i18n-sync diff
    ```

1. If everything is OK, you need to send the changed texts or statuses into the tanker, so that the documentation team can take a look at them. Push with ticket creation (`--ticket`):

    ```bash
    npx i18n-sync push --ticket
    ```

1. After the documentation team have looked at the ticket, you can unload the texts back and make a pull requeste. Unload texts from the tanker with:

    ```bash
    npx i18n-sync pull
    ```
