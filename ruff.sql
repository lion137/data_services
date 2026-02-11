[tool.ruff]
# Ruff should parse code assuming the *lowest* supported runtime.
# This impacts formatting and which syntax/features are allowed.
target-version = "py39"

# Match Black's default line length, unless your project uses another.
line-length = 88

# What files Ruff should include/exclude. Tweak as needed.
extend-exclude = [
  ".venv",
  "venv",
  "dist",
  "build",
  "__pypackages__",
]

[tool.ruff.format]
# Ruff formatter is Black-compatible in style.
quote-style = "double"
indent-style = "space"
line-ending = "auto"
skip-magic-trailing-comma = false

[tool.ruff.lint]
# A practical baseline that replaces most Flake8 setups.
# (You can expand later if you want stricter rules.)
select = [
  "E",   # pycodestyle errors
  "W",   # pycodestyle warnings
  "F",   # pyflakes
  "I",   # isort (import sorting)
  "B",   # flake8-bugbear
  "UP",  # pyupgrade (respects target-version)
  "SIM", # flake8-simplify
  "C4",  # flake8-comprehensions
]

# Rules commonly disabled to match Black / reduce noise.
ignore = [
  "E203", # whitespace before ':' (Black disagrees)
  "E501", # line length (formatter handles it)
]

# Apply fixes when you run `ruff check --fix`
fixable = ["ALL"]
unfixable = []

# If you use __init__.py exports patterns, this helps.
# (Optional)
# per-file-ignores = { "__init__.py" = ["F401"] }

[tool.ruff.lint.isort]
# Reasonable defaults; tune if you have a "src" layout or extra first-party packages.
combine-as-imports = true
force-sort-within-sections = true
known-first-party = ["src"]

[tool.ruff.lint.mccabe]
# Optional complexity gate (flake8 used to do this with mccabe)
max-complexity = 10

