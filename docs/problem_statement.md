# Problem Statement: ESG‑NLP Greenwashing Detector

## 1 Context
Global investors …

## 2 Problem
Manual review is slow and subjective …

## 3 Goal
Develop an NLP‑powered system …

## 4 Scope
S&P 500 10‑K *Item 1/1A* + CSR PDFs …

## 5 Success Metrics
- **KR 1.1**  Precision ≥ 75 % on validation set of 2023 filings  
- **KR 1.2**  Recall ≥ 70 %  
- **KR 2.1**  Spearman ρ ≥ 0.6 vs MSCI ESG ratings  
- **KR 3.1**  ≥ 20 % reduction in analyst review time (time‑and‑motion)

## 6 Constraints & Assumptions
- WSL 2 GPU (Quadro T1000, CUDA 12.7)  
- 100 % open‑source stack  
- No redistribution of full 10‑K text outside secure storage  
