# 📺 个人 TVBox 影视源

从公开上游聚合，经健康检测后保留高质量源，自动生成 TVBox 可用配置。

## 🚀 使用方式

在 TVBox / 影视仓 的「设置 → 配置地址」填入：

```
https://raw.githubusercontent.com/wan7up/tvbox-source/main/output/tvbox.json
```

单仓聚合，开箱即用。如需切换源，用多仓订阅：

```
https://raw.githubusercontent.com/wan7up/tvbox-source/main/output/urls.json
```

国内 raw.githubusercontent.com 慢可套 gh-proxy 前缀。

## 源可用性状态

<!-- STATUS_TABLE_START -->
> 🔄 最近更新：2026-11-02 01:29:07（北京时间）· 成功 **8/9** 个源

| 配置源 | 状态 | 站点数 | 延迟 | 备注 |
| --- | --- | --- | --- | --- |
| 肥猫 | ✅ | 39 | 1.389s |  |
| 王二小 | ✅ | 63 | 0.972s |  |
| 老刘备 | ✅ | 195 | 0.753s |  |
| 4K小盒子 | ✅ | 17 | 0.475s |  |
| 小盒子单仓 | ✅ | 54 | 0.48s |  |
| 拾光多仓 | ✅ | 0 | 0.888s | 多仓订阅 |
| 挺好分享多仓 | ✅ | 0 | 1.627s | 多仓订阅 |
| 饭太硬 | ❌ | 0 | - | JSON解析失败 |
| 小马 | ✅ | 20 | 11.711s |  |
<!-- STATUS_TABLE_END -->

## 🔄 自动更新

GitHub Actions 每天北京时间 10:00 和 22:00 自动运行：
1. 拉取所有上游配置
2. 站点级健康检查（CMS 调接口验返回，爬虫源验 ext/jar 可达）
3. 按指纹去重合并，连续失败 3 次的上游自动隔离
4. 输出聚合配置并更新状态表

也可手动触发：Actions → Update TVBox Sources → Run workflow。

## ⚙️ 自定义上游

编辑 `config/upstreams.json` 增删上游：
```json
{
  "id": "mysource",
  "name": "我的源",
  "urls": ["https://example.com/tv", "https://backup.example.com/tv"],
  "enabled": true
}
```
推送到 main 即触发更新。

## ⚠️ 免责声明

- 本仓库仅聚合互联网公开资源，不提供任何影视文件
- 所有资源仅供学习交流，请勿用于商业用途
- 如有侵权请联系删除
