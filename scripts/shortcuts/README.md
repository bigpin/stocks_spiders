# 快捷方式维护脚本

用于管理微信云开发中 `shortcut` 集合的快捷方式数据。依赖同目录上级的 `cloudbase_lib`，需配置 `.env`（`CLOUDBASE_ENV_ID`、`WECHAT_APPID`、`WECHAT_APPSECRET`）。

**使用前**：在云开发控制台创建集合 `shortcut`（权限可按需设置）。

**icon 字段**：每条快捷方式可存 `icon`（图片链接）。添加时若 `--url` 为网页地址且未传 `--icon`，会自动请求该页并抓取 `og:image` / `apple-touch-icon` / `icon` 作为 icon 写入；也可用 `--icon "https://..."` 直接指定。加 `--no-fetch-icon` 可关闭自动抓取。

## list_shortcuts.py

列出快捷方式。

```bash
# 列出全部
python list_shortcuts.py --list

# 按关键词过滤
python list_shortcuts.py --list --keyword "打卡"
```

## manage_shortcuts.py

添加、更新、删除、重置点击次数。

```bash
# 添加
python manage_shortcuts.py --add --name "打卡" --url "shortcuts://run-shortcut?name=xxx" --keywords "打卡,考勤"

# 更新
python manage_shortcuts.py --update --id <_id> --name "新名称"

# 删除
python manage_shortcuts.py --delete --id <_id>

# 重置点击次数
python manage_shortcuts.py --reset-clicks --id <_id>
```

## export_shortcuts.py

导出为 JSON 或 CSV 备份。

```bash
python export_shortcuts.py --output shortcuts.json
python export_shortcuts.py --csv --output shortcuts.csv
```
