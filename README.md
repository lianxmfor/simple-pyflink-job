a simple pyflink job

inputs:
```text
{"body":{"log": {"uid": "16297","contentstr": "{\"param\":{\"tag\": \"点击帖子卡片\"},\"postId\": \"100\"}","serverts": "1621428986"}}}
{"body":{"log": {"uid": "16297","contentstr": "{\"param\":{\"tag\": \"点击帖子卡片\"},\"postId\": \"200\"}","serverts": "1621428986"}}}
{"body":{"log": {"uid": "16297","contentstr": "{\"param\":{\"tag\": \"点击帖子卡片\"},\"postId\": \"300\"}","serverts": "1621428986"}}}
{"body":{"log": {"uid": "16297","contentstr": "{\"param\":{\"tag\": \"点击帖子卡片\"},\"postId\": \"400\"}","serverts": "1621428986"}}}
{"body":{"log": {"uid": "16297","contentstr": "{\"param\":{\"tag\": \"点击帖子卡片\"},\"postId\": \"500\"}","serverts": "1621428986"}}}
```

output in kafaka topic:

```text
{"user_id":16297,"datetime":"2021-05-19 20:56:26","features":"last_5_clicks=100,200,300,400,500"}
```
