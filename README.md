# sync-infra-configurations

## Usage

    $ sync-infra-configurations get SRC_FILE [DST_FILE]
    $ sync-infra-configurations get --diff SRC_FILE
    $ sync-infra-configurations get -i SRC_FILE

    $ sync-infra-configurations preview SRC_FILE [DST_FILE]
    $ sync-infra-configurations preview --diff SRC_FILE

    $ sync-infra-configurations update SRC_FILE [DST_FILE]
    $ sync-infra-configurations update --diff SRC_FILE

getコマンドは SRC_FILE の内容をクラウド側に問い合わせ表示する。 --diff を付けると SRC_FILE とクラウド側の差分をdiff形式で表示する。
-i を付けると、表示内容を SRC_FILE に書き戻す。

previewコマンドは SRC_FILE の内容をクラウド側に問い合わせ、差分のあった個所を反映するAPIの内容を SRC_FILE の中に埋め込んで表示する。
--diff を付けるとクラウド側と反映するAPIの内容を埋め込んだ SRC_FILE の差分をdiff形式で表示する。

updateコマンドは SRC_FILE の内容をクラウド側に問い合わせ、差分のあった個所をクラウド側に反映し、APIの内容を SRC_FILE の中に埋め込んで表示する。
--diff を付けるとクラウド側と反映するAPIの内容を埋め込んだ SRC_FILE の差分をdiff形式で表示する。
updateコマンドに --dry-run を付けると、previewコマンドと同じになる。

## Installation

    $ pip install git+https://github.com/suzuki-navi/sync-infra-configurations

## Development

    $ pip install -e .

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

