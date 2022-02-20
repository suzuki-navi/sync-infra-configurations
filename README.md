# sync-infra-configurations

## Usage

    $ sync-infra-configurations [get] [-y|--yaml] aws [--profile AWS_PROFILE] [-p PATH]
    #$ sync-infra-configurations get --repeat N aws [--profile AWS_PROFILE] [-p PATH]
    #$ sync-infra-configurations put aws [--profile AWS_PROFILE] [-p PATH] [--dry-run] < foo.txt

    #$ sync-infra-configurations get [--repeat N] -s SRC_FILE
    $ sync-infra-configurations get --diff -s SRC_FILE
    #$ sync-infra-configurations get -i [--repeat N] -s SRC_FILE

    #$ sync-infra-configurations put [--dry-run] -s SRC_FILE

    #$ sync-infra-configurations exec --aws [--profile AWS_PROFILE] [-p QUERY]

getコマンドはクラウド側の情報を取得してローカルにYAMLで出力する。

putコマンドはローカルのYAMLファイルをクラウド側に反映する。

SRC_FILE はローカルのYAMLファイルを指定する。
getコマンドでは SRC_FILE に書かれているリソースについてクラウド側に問い合わせをする。
putコマンドでは SRC_FILE に書かれているリソースをクラウド側に反映する。

getコマンドに--diffオプションを付けると、出力形式がdiff形式になる。put --dry-runコマンドでは標準でdiff形式と。
get --diff はput --dry-runコマンドの反対の出力になる。

#getコマンドに-iオプションを付けると、出力先が入力元と同じSRC_FILEになる。


## Installation

    $ pip install git+https://github.com/suzuki-navi/sync-infra-configurations

## Development

    $ pip install -e .

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

