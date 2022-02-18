# sync-infra-configurations

## Usage

    $ sync-infra-configurations get SRC_FILE [DST_FILE]
    $ sync-infra-configurations get --diff SRC_FILE
    $ sync-infra-configurations get -i SRC_FILE

    $ sync-infra-configurations preview SRC_FILE [DST_FILE]
    $ sync-infra-configurations preview --no-diff SRC_FILE

    $ sync-infra-configurations put SRC_FILE [DST_FILE]
    $ sync-infra-configurations put --no-diff SRC_FILE

getコマンドはクラウド側の情報を取得してローカルにYAMLで出力する。

previewコマンドは put --dry-run と同じ。

putコマンドはローカルのYAMLファイルをクラウド側に反映する。

SRC_FILE はローカルのYAMLファイルを指定する。
getコマンドでは SRC_FILE に書かれているリソースについてクラウド側に問い合わせをする。
putコマンドでは SRC_FILE に書かれているリソースをクラウド側に反映する。

DST_FILE は出力先となるローカルのファイル名を指定する。
指定がない場合は標準出力となる。

getコマンドに--diffオプションを付けると、出力形式がdiff形式になる。putコマンドでは標準でdiff形式となり、--no-diffオプションを付けるとYAML形式になる。

getコマンドに-iオプションを付けると、出力先が入力元と同じSRC_FILEになる。

## Installation

    $ pip install git+https://github.com/suzuki-navi/sync-infra-configurations

## Development

    $ pip install -e .

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

