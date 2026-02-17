repos:
  - repo: local
    hooks:
      - id: ruff-check
        name: ruff check .
        entry: ruff check .
        language: system
        pass_filenames: false

      - id: ruff-format-check
        name: ruff format --check .
        entry: ruff format --check .
        language: system
        pass_filenames: false

      - id: mypy-src
        name: mypy src
        entry: mypy src
        language: system
        pass_filenames: false

      - id: mypy-tests
        name: mypy tests
        entry: mypy tests
        language: system
        pass_filenames: false

      - id: pytest
        name: pytest
        entry: pytest
        language: system
        pass_filenames: false
        
        
        

$ErrorActionPreference = "Stop"

$PythonBin = if ($env:PYTHON_BIN) { $env:PYTHON_BIN } else { "python" }
$VenvDir   = if ($env:VENV_DIR) { $env:VENV_DIR } else { ".venv" }

Write-Host "==> Create venv: $VenvDir"
& $PythonBin -m venv $VenvDir

$Activate = Join-Path $VenvDir "Scripts\Activate.ps1"
Write-Host "==> Activate venv: $Activate"
& $Activate

Write-Host "==> Upgrade pip"
python -m pip install --upgrade pip

Write-Host "==> Install project (editable) with dev extras"
python -m pip install -e ".[dev]"

Write-Host "==> Install git hooks (pre-commit)"
python -m pre_commit install

Write-Host "==> Run all hooks once (sanity check)"
python -m pre_commit run --all-files

Write-Host "✅ Done."
Write-Host "Activate with: $Activate"
