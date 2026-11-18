# EDINET原本取得

金融庁EDINETの有価証券報告書・四半期報告書・半期報告書とその訂正書類を、既存の非公開R2に保存するGitHub Actionsです。初期の対象提出期間は2016年11月14日から2026年11月13日までです。

Actionsの「EDINET download」から手動で起動します。同時取得は1件、要求開始の最短間隔は0.5秒です。R2の取得済み記録を読み込み、保存済みの原本をスキップします。100書類、約2分、または処理終了を契機に変更分をまとめて保存します。

実行には既存のR2チェックポイントと、リポジトリのActions Secretsに設定した `EDINET_API_KEY`、`SWING_S3_BUCKET`、`SWING_S3_ENDPOINT`、`AWS_ACCESS_KEY_ID`、`AWS_SECRET_ACCESS_KEY` が必要です。最初の動作確認は `max_docs=1`、`max_days=1`、`minutes=3` で実行できます。
