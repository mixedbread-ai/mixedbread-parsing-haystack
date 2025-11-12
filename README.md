# mixedbread-parsing-haystack

**Quickstart**

1. Create an account on [mixedbread.com](https://platform.mixedbread.com)
2. Create an API Key (on the left -> API Keys -> Create)
3. Install the package: `pip install git+`
4. Using it:

```python
from mixedbread_parsing_haystack.converter import MixedbreadFileConverter

converter = MixedbreadFileConverter(api_key="")

document = converter.run(paths=["example.pdf"])
```