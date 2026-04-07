#!/usr/bin/env python3
"""
Reflow Javadoc comment paragraphs to fill lines close to 80 chars.

Detects "awkward" Javadoc lines where a paragraph continuation line
is much shorter than it could be (orphaned words), and reflows the
paragraph to balance line lengths.

Only touches plain Javadoc prose lines (starting with ' * '). Does
NOT touch:
- @param, @return, @throws, @link, @code tags
- Blank comment lines (' *')
- Lines with HTML tags (<p>, <ul>, <li>, <pre>, etc.)
- Lines with code examples or formatting
- The opening '/**' and closing '*/' lines
"""

import os
import re
import sys


MAX_LINE = 80


def is_prose_line(stripped):
    """Return True if this is a plain Javadoc prose line."""
    if not stripped.startswith('* '):
        return False
    content = stripped[2:].strip()
    if not content:
        return False
    # Skip tag lines
    if content.startswith('@'):
        return False
    # Skip HTML tags
    if content.startswith('<') or content.startswith('{@'):
        return False
    # Skip CSOFF/CSON
    if content.startswith('CSOFF') or content.startswith('CSON'):
        return False
    return True


def is_tag_continuation(stripped):
    """Return True if this looks like a continuation of a @tag."""
    if not stripped.startswith('* '):
        return False
    content = stripped[2:].strip()
    # Tag continuation lines are indented with extra spaces
    # after the '* ' prefix
    raw_after_star = stripped[2:] if stripped.startswith('* ') else ''
    if raw_after_star.startswith('  '):
        return True
    return False


def reflow_paragraph(lines, prefix):
    """Reflow a list of prose strings into filled lines.

    Args:
        lines: List of prose text strings (without the prefix).
        prefix: The comment prefix (e.g., '     * ').

    Returns:
        List of reflowed lines (with prefix and newline).
    """
    # Join all words
    words = []
    for line in lines:
        words.extend(line.split())

    if not words:
        return [prefix + line + '\n' for line in lines]

    max_content = MAX_LINE - len(prefix)

    result = []
    current = words[0]

    for word in words[1:]:
        test = current + ' ' + word
        if len(test) <= max_content:
            current = test
        else:
            result.append(prefix + current + '\n')
            current = word

    result.append(prefix + current + '\n')
    return result


def has_short_line(lines, prefix):
    """Check if any line in the paragraph is awkwardly short
    (could fit more words from the next line)."""
    max_content = MAX_LINE - len(prefix)

    for i in range(len(lines) - 1):
        current_len = len(lines[i])
        next_first_word = lines[i + 1].split()[0] \
            if lines[i + 1].split() else ''

        if current_len + 1 + len(next_first_word) <= max_content:
            return True

    return False


def process_file(filepath):
    """Process a single Java file, reflowing awkward Javadoc."""
    with open(filepath, 'r', encoding='utf-8') as f:
        original_lines = f.readlines()

    new_lines = []
    changed = False
    i = 0

    while i < len(original_lines):
        line = original_lines[i]
        rstripped = line.rstrip('\n').rstrip('\r')
        stripped = rstripped.strip()

        # Check if this is a prose Javadoc line
        if not is_prose_line(stripped):
            new_lines.append(line)
            i += 1
            continue

        # Determine the prefix (indentation + '* ')
        indent = rstripped[:rstripped.index('*')]
        prefix = indent + '* '

        # Collect consecutive prose lines into a paragraph
        para_texts = []
        para_start = i

        # Add the first line (already confirmed as prose)
        text = stripped[2:].strip()
        para_texts.append(text)
        i += 1

        while i < len(original_lines):
            l = original_lines[i].rstrip('\n').rstrip('\r')
            s = l.strip()

            if is_prose_line(s) and not is_tag_continuation(s):
                text = s[2:].strip()
                para_texts.append(text)
                i += 1
            else:
                break

        # Only reflow if there are multiple lines and
        # the paragraph has awkward short lines
        if len(para_texts) > 1 \
                and has_short_line(para_texts, prefix):
            reflowed = reflow_paragraph(para_texts, prefix)
            new_lines.extend(reflowed)
            if len(reflowed) != len(para_texts):
                changed = True
            else:
                # Check if content differs
                for j, rl in enumerate(reflowed):
                    orig = original_lines[para_start + j]
                    if rl != orig:
                        changed = True
                        break
        else:
            # Keep original lines
            for j in range(para_start, i):
                new_lines.append(original_lines[j])

    if changed:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.writelines(new_lines)
        return True
    return False


def main():
    src_dir = 'src/main/java'
    if not os.path.isdir(src_dir):
        print(f"ERROR: {src_dir} not found.")
        sys.exit(1)

    total = 0
    modified = 0

    for root, dirs, files in os.walk(src_dir):
        for fname in sorted(files):
            if not fname.endswith('.java'):
                continue
            filepath = os.path.join(root, fname)
            total += 1
            if process_file(filepath):
                modified += 1
                print(f"  Fixed: {filepath}")

    print(f"\nProcessed {total} files, modified {modified}.")


if __name__ == '__main__':
    main()
