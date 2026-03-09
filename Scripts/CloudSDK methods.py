#!/usr/bin/env python3
"""
Find 'operational' functions/methods in the eecloud package, e.g. simulation progress/monitor.
Usage:
  python find_eecloud_ops.py
  python find_eecloud_ops.py --keywords simulation,progress,monitor
"""

import argparse
import importlib
import inspect
import pkgutil
import sys
from types import ModuleType
from typing import Iterable, List, Tuple, Set

BLOCKLIST_NAMES: Set[str] = {
    # common model/serialization/boilerplate
    "to_dict", "from_dict", "to_json", "from_json", "schema", "dict", "json",
    "copy", "model_dump", "model_json", "parse_obj", "validate", "schema_json",
    "model_validate", "model_validate_json", "model_dump_json",
    # dunder-ish (we skip anything starting with _ anyway)
}

DEFAULT_KEYWORDS = ["simulation", "simulate", "progress", "monitor", "run", "job"]

def safe_import(dotted: str):
    try:
        return importlib.import_module(dotted)
    except Exception as e:
        print(f"[!] Could not import '{dotted}': {e}", file=sys.stderr)
        return None

def public_name(name: str) -> bool:
    return not name.startswith("_") and name not in BLOCKLIST_NAMES

def matches_keywords(name: str, keywords: Iterable[str]) -> bool:
    if not keywords:
        return True
    lname = name.lower()
    return any(k.strip().lower() in lname for k in keywords if k.strip())

def signature_of(obj) -> str:
    try:
        return str(inspect.signature(obj))
    except Exception:
        return "(...)"

def oneline_doc(obj, max_len=100) -> str:
    doc = inspect.getdoc(obj) or ""
    first = doc.strip().splitlines()[0] if doc else ""
    return (first[: max_len - 1] + "…") if len(first) > max_len else first

def list_module_functions(mod: ModuleType, keywords: Iterable[str]):
    for name, obj in inspect.getmembers(mod, lambda o: inspect.isfunction(o) or inspect.isbuiltin(o)):
        if getattr(obj, "__module__", "").split(".")[0] != mod.__name__.split(".")[0]:
            # Skip foreign functions pulled in from other packages
            continue
        if public_name(name) and matches_keywords(name, keywords):
            yield (f"{mod.__name__}.{name}", obj)

def list_module_classes(mod: ModuleType):
    for name, cls in inspect.getmembers(mod, inspect.isclass):
        if getattr(cls, "__module__", "").startswith(mod.__name__):
            if public_name(name):
                yield cls

def list_class_methods(cls, keywords: Iterable[str]):
    seen = set()
    for name, member in inspect.getmembers(cls):
        is_call = (
            inspect.isfunction(member)
            or inspect.ismethod(member)
            or inspect.ismethoddescriptor(member)
            or inspect.isbuiltin(member)
        )
        if not is_call or not public_name(name) or not matches_keywords(name, keywords):
            continue
        # de-dupe across MRO
        if name in seen:
            continue
        seen.add(name)
        # where defined?
        owner = next((base for base in cls.__mro__ if name in getattr(base, "__dict__", {})), cls)
        yield (name, owner.__name__, member)

def walk_package(root_pkg_name: str):
    root = safe_import(root_pkg_name)
    if not root or not hasattr(root, "__path__"):
        return []
    mods = [root]
    for finder, name, ispkg in pkgutil.walk_packages(root.__path__, root.__name__ + "."):
        m = safe_import(name)
        if m:
            mods.append(m)
    return mods

def print_heading(text: str):
    print("\n" + text)
    print("-" * len(text))

def main():
    ap = argparse.ArgumentParser(description="List operational APIs in eecloud.")
    ap.add_argument("--keywords", type=str, default=",".join(DEFAULT_KEYWORDS),
                    help="Comma-separated keywords to include (case-insensitive). Empty for no filter.")
    args = ap.parse_args()
    keywords = [k.strip() for k in (args.keywords.split(",") if args.keywords is not None else []) if k.strip()]

    # 1) Scan the whole eecloud package
    modules = walk_package("eecloud")
    if not modules:
        print("[!] Could not import 'eecloud' or it has no submodules.", file=sys.stderr)
        sys.exit(1)

    # 2) Module-level functions
    print_heading("Module-level functions matching keywords")
    found_any = False
    for mod in sorted(modules, key=lambda m: m.__name__):
        rows = list(list_module_functions(mod, keywords))
        if not rows:
            continue
        found_any = True
        print(f"\n[{mod.__name__}]")
        for fqname, obj in sorted(rows, key=lambda t: t[0]):
            print(f"  - {fqname}{signature_of(obj)}")
            doc = oneline_doc(obj)
            if doc:
                print(f"      {doc}")
    if not found_any:
        print("(none found)")

    # 3) Classes & their methods
    print_heading("Class methods matching keywords")
    found_any = False
    for mod in sorted(modules, key=lambda m: m.__name__):
        classes = list(list_module_classes(mod))
        if not classes:
            continue
        mod_printed = False
        for cls in classes:
            methods = list(list_class_methods(cls, keywords))
            if not methods:
                continue
            found_any = True
            if not mod_printed:
                print(f"\n[{mod.__name__}]")
                mod_printed = True
            print(f"  {cls.__module__}.{cls.__name__}")
            for name, owner, member in sorted(methods, key=lambda t: (t[1], t[0])):
                owner_hint = f"  [defined on {owner}]" if owner != cls.__name__ else ""
                print(f"    - {name}{signature_of(member)}{owner_hint}")
                doc = oneline_doc(member)
                if doc:
                    print(f"        {doc}")

    if not found_any:
        print("(none found)")

    # 4) CloudSDK class (explicit)
    cloudsdk_mod = safe_import("eecloud.cloudsdk")
    if cloudsdk_mod and hasattr(cloudsdk_mod, "CloudSDK"):
        CloudSDK = getattr(cloudsdk_mod, "CloudSDK")
        if inspect.isclass(CloudSDK):
            print_heading("eecloud.cloudsdk.CloudSDK methods (filtered)")
            methods = list(list_class_methods(CloudSDK, keywords))
            if methods:
                for name, owner, member in sorted(methods, key=lambda t: (t[1], t[0])):
                    owner_hint = f"  [defined on {owner}]" if owner != CloudSDK.__name__ else ""
                    print(f"  - {name}{signature_of(member)}{owner_hint}")
                    doc = oneline_doc(member)
                    if doc:
                        print(f"      {doc}")
            else:
                print("(no matching methods on CloudSDK)")

if __name__ == "__main__":
    main()
