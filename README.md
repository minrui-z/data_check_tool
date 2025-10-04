# 樣本檢查工具 (Windows 版) v1

本程式用於自動化檢查訪次與樣本資料，協助輔導員初步檢查訪員訪問情形。

## 功能
- 自動檢查各樣本的訪次紀錄是否完整。
- 驗證問卷是否有未填寫或異常情況。
- 輸出包含檢查結果的報表，方便後續追蹤與修正。

## 使用方式

### 1. 環境需求
- Windows 10以上作業系統

### 2. 執行程式
請於Release下載```sample_checker.exe```，後點擊執行，若有簽署需求請依循下列步驟：
```
# 1. 安裝 cosign (一次性)
# Windows: 到 https://github.com/sigstore/cosign/releases 下載 cosign-windows-amd64.exe
# 重新命名為 cosign.exe 並加入 PATH

# 2. 把三個檔案放在同一個資料夾
# - sample_checker.exe
# - sample_checker.exe.sig
# - sample_checker.exe.pem

# 3. 執行驗證
cosign verify-blob ^
  --certificate sample_checker.exe.pem ^
  --signature sample_checker.exe.sig ^
  --certificate-identity-regexp "https://github.com/minrui-z/data_check_tool/.*" ^
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com" ^
  sample_checker.exe
```

### 3. 程式流程
1. 程式會讀取樣本與訪次資料。
2. 自動執行多項檢查規則（如問卷填寫完整度、結果代碼正確性）。
3. 將檢查結果輸出為 CSV 檔案，供輔導員檢閱，請檢查資料正確性並整理過再請訪員修正。
4. 

### 4. 輸出結果
輸出報表將包含：
- 樣本編號
- 訪員姓名
- 訪問日期
- 結果代碼
- 問題描述
- 檢查類別

## 注意事項
- 請確認輸入資料格式正確，避免編碼或欄位名稱錯誤。
- 若有更新版本，建議及時更新以獲得最新檢查規則。

## 版本資訊
- v1.0.0：初始發佈，提供樣本檢查與輸出報表功能。
- v1.0.1：更新樣本檢查規則，有100代碼的樣本，前面的訪次將不再誤判填錯。
- v1.1.0：更新GUI，使介面較為現代化
