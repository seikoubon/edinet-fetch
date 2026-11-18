# EDINET原本取得

金融庁EDINETの有価証券報告書・四半期報告書・半期報告書とその訂正書類を、既存の非公開R2に保存するGitHub Actionsです。初期の対象提出期間は2016年11月14日から2026年11月13日までです。

Actionsの「EDINET download」から手動で起動します。同時取得は1件、要求開始の最短間隔は0.5秒です。R2の取得済み記録を読み込み、保存済みの原本をスキップします。100書類、約2分、または処理終了を契機に変更分をまとめて保存します。

実行には既存のR2チェックポイントと、リポジトリのActions Secretsに設定した `EDINET_API_KEY`、`SWING_S3_BUCKET`、`SWING_S3_ENDPOINT`、`AWS_ACCESS_KEY_ID`、`AWS_SECRET_ACCESS_KEY` が必要です。最初の動作確認は `max_docs=1`、`max_days=1`、`minutes=3` で実行できます。

## Queriaの財務データ

Actionsの「Queria financial data」で、2016年11月14日から実行前日（日本時間）までの証券コード付き有報・半期報告書・四半期報告書と訂正の一覧、Queriaが提供する主要経営指標・研究開発費・連結子会社数・人的資本を取得します。Parquetと取得版・件数を含むmanifestは非公開R2の `japan-swing/queria/bulk/v1/` に保存します。同じ対象期間・同じ配信版の公開済みスナップショットを再利用します。

出典は金融庁EDINET、加工は[Queria](https://queria.io/datasets/queria/edinet)です。提出書類の一覧と財務数値の収録範囲は異なります。欠損はそのまま保持します。過去の戦略検証では書類の提出日時と訂正履歴を確認してください。
