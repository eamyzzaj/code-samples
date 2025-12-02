import nltk
from collections import defaultdict
import pandas as pd


def extract_title_xml(single_lit_work):
    xml_tree = single_lit_work["work_xml"]
    single_lit_work["work_name"] = xml_tree.find('.//TITLE').text 

# -----------------------------------------------
# 1. Extract character data
# -----------------------------------------------
def extract_charcs_xml(lit_work: list, print_charcs=False):
    main_charcs = []
    side_charcs = []

    for p in lit_work:
        for l in p.itertext():
            if p.tag == "TITLE":
                title = l.strip()
        if p.tag == "PERSONAE":
            lines = [l.strip() for l in p.itertext() if l.strip()]
            if lines and "Dramatis Personae" in lines[0]:
                lines.pop(0)

            group = []

            for line in lines:
                if line.isupper():
                    group.append(line)
                    continue

                if group:
                    desc = line.strip(". ")
                    prefix = "(of the group) " if len(group) > 1 else ""
                    for name in group:
                        main_charcs.append({"name": name, "desc": prefix + desc})
                    group = []
                    continue

                if "," in line:
                    name_part, desc_part = line.split(",", 1)
                    name_part, desc_part = name_part.strip(), desc_part.strip(". ")
                    first_word = name_part.split()[0]
                    if first_word.isupper():
                        main_charcs.append({"name": name_part, "desc": desc_part})
                    else:
                        side_charcs.append({"desc": line.strip(". ")})
                    continue

                first_word = line.split()[0]
                if first_word.isupper():
                    main_charcs.append({"name": line, "desc": ""})
                else:
                    side_charcs.append({"desc": line.strip(". ")})

            for name in group:
                main_charcs.append({"name": name, "desc": ""})

    if print_charcs:
        print("=" * 50)
        print(f"{title}")
        print("=" * 50)
        print("Main Characters:")
        for c in main_charcs:
            print(f" - {c['name']}: {c['desc']}")
        print("\nSide Characters:")
        for c in side_charcs:
            print(f" - {c['desc']}")
        print("\n\n")

    return main_charcs, side_charcs


# -----------------------------------------------
# 2. Parse play XML
# -----------------------------------------------
def parse_play_xml(xml_tree):
    """
    Parses a Shakespeare XML play into a structured dictionary.
    Includes acts, scenes, and per-scene speech + line counts.
    """
    root = xml_tree
    title = root.find('.//TITLE').text if root.find('.//TITLE') is not None else "Unknown Title"

    chars = root.findall('.//PERSONA')
    group_chars = root.findall('.//PGROUP')
    acts = root.findall('.//ACT')

    main_characters = []
    for char in chars:
        for l in char.itertext():
            l = l.strip()
            if not l:
                continue
            if ',' in l:
                main, desc = l.split(',', 1)
                if main.split()[0].isupper():
                    main_characters.append({
                        "name": main.strip(),
                        "desc": desc.strip()
                    })
            else:
                main_characters.append({"name": l, "desc": ""})

    group_characters = []
    for gchar in group_chars:
        desc_elem = gchar.find('.//GRPDESCR')
        desc = desc_elem.text.strip() if desc_elem is not None else "(in group)"
        for char in gchar.findall('.//PERSONA'):
            for l in char.itertext():
                l = l.strip()
                if not l:
                    continue
                group_characters.append({
                    "name": l,
                    "group_desc": f"(in group) {desc}"
                })

    # --- Extract acts, scenes, and counts ---
    act_data = []
    for act_i, act in enumerate(acts, start=1):
        scenes = act.findall('.//SCENE')
        scene_data = []

        for scene_i, scene in enumerate(scenes, start=1):
            speech_stats = {}
            speakers = []

            for speech in scene.findall('.//SPEECH'):
                line_count = len(speech.findall('.//LINE'))
                for s in speech.findall('.//SPEAKER'):
                    if not s.text:
                        continue
                    name = s.text.strip()
                    speakers.append(name)

                    if name not in speech_stats:
                        speech_stats[name] = {"speeches": 0, "lines": 0, "acts": set()}

                    speech_stats[name]["speeches"] += 1
                    speech_stats[name]["lines"] += line_count
                    speech_stats[name]["acts"].add(act_i)

            unique_speakers = sorted(set(speakers))
            scene_data.append({
                "scene": scene_i,
                "speakers": [
                    {
                        "name": spkr,
                        "speech_count": speech_stats.get(spkr, {}).get("speeches", 0),
                        "line_count": speech_stats.get(spkr, {}).get("lines", 0),
                        "acts_appeared": len(speech_stats.get(spkr, {}).get("acts", set()))
                    }
                    for spkr in unique_speakers
                ]
            })

        act_data.append({
            "act": act_i,
            "scenes": scene_data
        })

    return {
        "title": title,
        "acts": act_data,
        "main_characters": main_characters,
        "group_characters": group_characters
    }


# -----------------------------------------------
# 3. Merge data
# -----------------------------------------------
def merge_play_data(parsed_play, main_charcs, side_charcs):
    char_map = {}

    for c in main_charcs:
        name = (c.get("name") or "").strip().upper()
        if name:
            char_map[name] = c.get("desc", "")
    for c in side_charcs:
        name = (c.get("name") or "").strip().upper()
        if name:
            char_map[name] = c.get("desc", "")
        else:
            desc = c.get("desc", "")
            if desc:
                char_map[desc.upper()] = desc

    merged_acts = []
    for act in parsed_play["acts"]:
        act_entry = {"act": act["act"], "scenes": []}
        for scene in act["scenes"]:
            speakers = []
            for s in scene["speakers"]:
                name = s["name"].strip().upper()
                speakers.append({
                    "name": s["name"],
                    "speech_count": s.get("speech_count", 0),
                    "line_count": s.get("line_count", 0),
                    "acts_appeared": s.get("acts_appeared", 0),
                    "desc": char_map.get(name, "(no description found)")
                })
            act_entry["scenes"].append({
                "scene": scene["scene"],
                "speakers": speakers
            })
        merged_acts.append(act_entry)

    return {
        "title": parsed_play.get("title", "Unknown Play"),
        "acts": merged_acts,
        "characters": main_charcs + side_charcs
    }


# -----------------------------------------------
# 4. Summarize quantitative metrics
# -----------------------------------------------
from dataclasses import dataclass, field
from collections import defaultdict
import pandas as pd

@dataclass
class CharStats:
    speeches: int = 0
    lines: int = 0
    scenes: set[tuple[int, int]] = field(default_factory=set)
    acts: set[int] = field(default_factory=set)


def summarize_play_stats(merged_play, main_charcs=None, side_charcs=None, print_summary=True):
    """Summarizes quantitative statistics from a merged Shakespeare play."""
    totals: dict[str, CharStats] = defaultdict(CharStats)
    total_speeches = 0
    total_lines = 0
    total_scenes = 0
    act_speech_totals: dict[int, int] = {}

    # --- Collect totals ---
    for act in merged_play["acts"]:
        act_index = act["act"]
        act_total = 0
        for scene in act["scenes"]:
            scene_index = scene["scene"]
            total_scenes += 1
            for s in scene["speakers"]:
                name = s["name"]
                speeches = s.get("speech_count", 0)
                lines = s.get("line_count", 0)

                stats = totals[name]
                stats.speeches += speeches
                stats.lines += lines
                stats.acts.add(act_index)
                stats.scenes.add((act_index, scene_index)) 
                total_speeches += speeches
                total_lines += lines
                act_total += speeches
        act_speech_totals[act_index] = act_total

    # --- Compute global totals ---
    total_acts = len(merged_play["acts"])
    main_count = len(main_charcs) if main_charcs else 0
    side_count = len(side_charcs) if side_charcs else 0
    main_side_ratio = f"{main_count}:{side_count}" if side_count else "N/A"

    # --- Build DataFrame ---
    data = []
    for name, stats in totals.items():
        acts_count = len(stats.acts)
        speeches = stats.speeches
        lines = stats.lines
        scenes = len(stats.scenes)

        speech_share = (speeches / total_speeches * 100) if total_speeches > 0 else 0
        line_share = (lines / total_lines * 100) if total_lines > 0 else 0
        avg_speeches_per_scene = speeches / scenes if scenes > 0 else 0
        avg_lines_per_speech = lines / speeches if speeches > 0 else 0
        scene_ratio = scenes / total_scenes if total_scenes > 0 else 0

        verbosity = avg_lines_per_speech
        talkativeness = avg_speeches_per_scene
        dominance = line_share
        focus = lines / acts_count if acts_count > 0 else 0
        breadth = scene_ratio

        role_type = "main" if main_charcs and any(
            name.upper() == c["name"].upper() for c in main_charcs
        ) else "side"

        data.append({
            "play": merged_play["title"],
            "character": name,
            "total_speeches": speeches,  # Character's speeches
            "total_lines": lines,  # Character's lines
            "scenes_appeared": scenes,
            "acts_appeared": acts_count,
            "speech_share_pct": round(speech_share, 2),
            "line_share_pct": round(line_share, 2),
            "avg_speeches_per_scene": round(avg_speeches_per_scene, 2),
            "avg_lines_per_speech": round(avg_lines_per_speech, 2),
            "verbosity": round(verbosity, 2),
            "talkativeness": round(talkativeness, 2),
            "dominance": round(dominance, 2),
            "focus": round(focus, 2),
            "breadth": round(breadth, 2),
            "role_type": role_type,
            "play_total_acts": total_acts,  # Renamed
            "play_total_scenes": total_scenes,  # Renamed
            "play_total_speeches": total_speeches,  # Renamed
            "play_total_lines": total_lines,  # Renamed
            "main_side_ratio": main_side_ratio  # Fixed typo
        })

    # --- Normalize names (spacing + case) ---
    for entry in data:
        entry["character"] = " ".join(entry["character"].split()).strip().upper()

    df = pd.DataFrame(data)

    # --- Merge duplicates regardless of Role Type ---
    agg_cols = {
        "total_speeches": "sum",
        "total_lines": "sum",
        "scenes_appeared": "sum",
        "acts_appeared": "max",
        "speech_share_pct": "mean",
        "line_share_pct": "mean",
        "avg_speeches_per_scene": "mean",
        "avg_lines_per_speech": "mean",
        "verbosity": "mean",
        "talkativeness": "mean",
        "dominance": "mean",
        "focus": "mean",
        "breadth": "mean",
        "play_total_acts": "first", 
        "play_total_scenes": "first",
        "play_total_speeches": "first",
        "play_total_lines": "first",
        "main_side_ratio": "first"
    }

    merged_df = (
        df.groupby(["play", "character"], as_index=False)
        .agg(agg_cols)
    )

    # Reassign Role Type: main if any entry for that character was main
    role_map = (
        df.groupby(["play", "character"])["role_type"]
          .apply(lambda x: "main" if "main" in x.values else "side")
          .reset_index()
    )

    df = merged_df.merge(role_map, on=["play", "character"], how="left")
    df = df.sort_values("total_lines", ascending=False).reset_index(drop=True)

    summary = {
        "play_title": merged_play["title"],
        "total_acts": total_acts,
        "total_scenes": total_scenes,
        "total_speeches": total_speeches,
        "total_lines": total_lines,
        "act_speech_totals": act_speech_totals,
        "main_side_ratio": main_side_ratio,
        "character_df": df
    }

    # --- Optional printout ---
    if print_summary:
        print(f"\n{summary['play_title']}")
        print("=" * (len(summary['play_title']) + 3))
        print(
            f"Acts: {total_acts} | Scenes: {total_scenes} | "
            f"Total Speeches: {total_speeches} | Total Lines: {total_lines}"
        )
        print(f"Main: {main_count} | Side: {side_count} | Ratio: {main_side_ratio}\n")

        print("Act-level speech totals:")
        for act, total in act_speech_totals.items():
            print(f"  Act {act}: {total} speeches")

        print("\nTop 10 characters by line count:")
        print(df.head(10).to_string(index=False))

    return summary


# -----------------------
# Networks
# -----------------------

import itertools
import pandas as pd
from collections import Counter

def normalize_name(name: str) -> str:
    """Uppercase, trim, and collapse multiple spaces."""
    return " ".join(str(name).strip().upper().split())


def build_cooccurrence_network_clean(merged_play):
    """
    Build a cleaned co-occurrence table for a play.
    Each row: pair of characters, scene count, act-scene list.
    Names normalized; reversed pairs deduplicated; scenes sorted numerically.
    """
    title = merged_play["title"]
    interactions = defaultdict(set)  # {(A,B): {(act,scene)}}

    for act in merged_play["acts"]:
        act_num = act["act"]
        for scene in act["scenes"]:
            scene_num = scene["scene"]
            speakers = [
                normalize_name(s["name"])
                for s in scene["speakers"]
                if s["name"]
            ]
            unique_speakers = sorted(set(speakers))

            # record co-occurrence for all pairs in this scene
            for a, b in itertools.combinations(unique_speakers, 2):
                key = tuple(sorted([a, b]))  # ensure (A,B) == (B,A)
                interactions[key].add((act_num, scene_num))

    # convert to DataFrame
    rows = []
    for (a, b), scenes in interactions.items():
        scene_list = sorted(f"{act}.{scene}" for act, scene in scenes)
        act_nums = sorted({int(a) for a, _ in scenes})
        scene_nums = sorted({int(s) for _, s in scenes})

        rows.append({
            "Play": title,
            "Character A": a,
            "Character B": b,
            "Scenes Together": len(scenes),
            "Scenes List": ", ".join(scene_list),
            "Acts Together": ", ".join(map(str, act_nums)),
            "Scenes Together (IDs)": ", ".join(map(str, scene_nums))
        })

    return pd.DataFrame(rows)


def build_networks_for_all(works):
    """
    For each play in `works`, build a cleaned co-occurrence network
    and save one CSV per play.
    """
    for w in works:
        xml_tree = w["work_xml"]
        play_name = w["work_name"]

        main, side = extract_charcs_xml(xml_tree, print_charcs=False)
        parsed = parse_play_xml(xml_tree)
        merged = merge_play_data(parsed, main, side)

        df_edges = build_cooccurrence_network_clean(merged)

        df_edges = df_edges.sort_values(
            ["Character A", "Character B"]
        ).reset_index(drop=True)

        out_path = f"../csv/{play_name.lower().replace(' ', '_')}_network.csv"
        df_edges.to_csv(out_path, index=False)
        print(f"Saved cleaned network for {play_name}: {out_path}")


import pandas as pd

def extract_speeches_and_lines_by_scene(xml_tree):
    """
    Extracts both speech-level and line-level data from a play XML tree.

    Returns two DataFrames:
        1. speeches_df: Play, Act, Scene, Character, Line Count, Text
        2. lines_df: Play, Act, Scene, Character, Line Number, Text
    """
    root = xml_tree
    title = root.find(".//TITLE").text if root.find(".//TITLE") is not None else "Unknown Play"

    speech_rows = []
    line_rows = []

    for act_i, act in enumerate(root.findall(".//ACT"), start=1):
        for scene_i, scene in enumerate(act.findall(".//SCENE"), start=1):
            for speech in scene.findall(".//SPEECH"):
                speakers = [normalize_name(s.text) for s in speech.findall(".//SPEAKER") if s.text]
                lines = [l.text.strip() for l in speech.findall(".//LINE") if l.text and l.text.strip()]
                if not speakers or not lines:
                    continue

                # Combine all lines for speech-level text
                speech_text = " ".join(lines)
                line_count = len(speech_text.split())

                for speaker in speakers:
                    # Add speech-level record
                    speech_rows.append({
                        "Play": title,
                        "Act": act_i,
                        "Scene": scene_i,
                        "Character": speaker,
                        "Line Count": line_count,
                        "Text": speech_text
                    })

                    # Add line-level records
                    for line_num, line_text in enumerate(lines, start=1):
                        line_rows.append({
                            "Play": title,
                            "Act": act_i,
                            "Scene": scene_i,
                            "Character": speaker,
                            "Line Number": line_num,
                            "Text": line_text
                        })

    return pd.DataFrame(speech_rows), pd.DataFrame(line_rows)


def extract_all_speeches_and_lines(works):
    """
    Extract speech-level and line-level data for each play.
    Saves both as separate CSVs per play.
    """
    for w in works:
        xml_tree = w["work_xml"]
        play_name = w["work_name"]
        print(f"Extracting speeches and lines for {play_name}...")

        speeches_df, lines_df = extract_speeches_and_lines_by_scene(xml_tree)

        base_name = play_name.lower().replace(" ", "_").replace("'", "")
        speech_path = f"../csv/{base_name}_speeches.csv"
        line_path = f"../csv/{base_name}_lines.csv"

        speeches_df.to_csv(speech_path, index=False)
        lines_df.to_csv(line_path, index=False)

        print(f"Saved {speech_path} ({len(speeches_df)} speeches)")
        print(f"Saved {line_path} ({len(lines_df)} lines)")


# -----------------------
# Quantitative Stats about Play
# -----------------------

import os
import pandas as pd

def count_story_lines(xml_tree):
    """
    Count only dialogue <LINE> elements inside <SPEECH> blocks.
    Returns (play_title, total_lines, total_speeches).
    """
    root = xml_tree
    title = root.find(".//TITLE").text if root.find(".//TITLE") is not None else "Unknown Play"

    line_count = 0
    speech_count = 0

    for speech in root.findall(".//SPEECH"):
        has_line = False
        for line in speech.findall(".//LINE"):
            text = line.text.strip() if line.text else ""
            if text:
                line_count += 1
                has_line = True
        if has_line:
            speech_count += 1

    return title, line_count, speech_count


def count_characters(lit_work: dict):
    """
    Count main and side characters defined in the dramatis personae.
    """
    main_char_ct = len(lit_work.get("main_charcs", []))
    side_char_ct = len(lit_work.get("side_charcs", []))
    total_char_ct = main_char_ct + side_char_ct
    return main_char_ct, side_char_ct, total_char_ct


def create_story_stats(works: list):
    """
    Creates both:
    - Play-level quantitative summaries (acts, scenes, speeches, etc.)
    - Scene-level layout summaries (act/scene + speeches, lines, unique characters)
    For each play in the list.
    Saves all outputs into ../csv/.
    """
    import os
    import pandas as pd

    os.makedirs("../csv", exist_ok=True)

    all_play_summaries = []  # store all play-level summaries together

    for lit_work in works:
        xml_tree = lit_work["work_xml"]
        play_title = lit_work["work_name"]

        # -----------------------
        # Count global structure
        # -----------------------
        acts = xml_tree.findall(".//ACT")
        act_count = len(acts)
        scene_count = sum(len(act.findall(".//SCENE")) for act in acts)

        # Dialogue totals
        title, line_count, speech_count = count_story_lines(xml_tree)

        # Character counts
        main_ct, side_ct, total_ct = count_characters(lit_work)

        # Derived averages
        avg_lines_scene = round(line_count / scene_count, 2) if scene_count else 0
        avg_speeches_scene = round(speech_count / scene_count, 2) if scene_count else 0
        avg_lines_speech = round(line_count / speech_count, 2) if speech_count else 0

        # -----------------------
        # Play-level summary
        # -----------------------
        summary_df = pd.DataFrame([{
            "Play": play_title,
            "Acts": act_count,
            "Scenes": scene_count,
            "Speeches": speech_count,
            "Dialogue Lines": line_count,
            "Main Characters": main_ct,
            "Side Characters": side_ct,
            "Total Characters": total_ct,
            "Avg Lines/Scene": avg_lines_scene,
            "Avg Speeches/Scene": avg_speeches_scene,
            "Avg Lines/Speech": avg_lines_speech
        }])
        all_play_summaries.append(summary_df)

        safe_name = play_title.lower().replace(" ", "_").replace("'", "")
        story_stats_path = f"../csv/{safe_name}_story_stats.csv"
        summary_df.to_csv(story_stats_path, index=False)
        print(f"Saved play summary: {story_stats_path}")

        # -----------------------
        # Scene-level layout (with cast size)
        # -----------------------
        layout_rows = []
        for act_i, act in enumerate(acts, start=1):
            scenes = act.findall(".//SCENE")
            print(f"{play_title} - Act {act_i}: {len(scenes)} scenes")

            for scene_i, scene in enumerate(scenes, start=1):
                speech_count_scene = 0
                line_count_scene = 0
                speakers_in_scene = set()

                for speech in scene.findall(".//SPEECH"):
                    lines = [l.text.strip() for l in speech.findall(".//LINE") if l.text and l.text.strip()]
                    speakers = [s.text.strip().upper() for s in speech.findall(".//SPEAKER") if s.text]

                    if lines:
                        speech_count_scene += 1
                        line_count_scene += len(lines)
                        speakers_in_scene.update(speakers)

                layout_rows.append({
                    "Play": play_title,
                    "Act": act_i,
                    "Scene": scene_i,
                    "Speeches": speech_count_scene,
                    "Dialogue Lines": line_count_scene,
                    "Unique Speakers": len(speakers_in_scene)
                })

        layout_df = pd.DataFrame(layout_rows)

        # Aggregate summaries per act
        act_summary = (
            layout_df.groupby("Act")
            .agg({
                "Scene": "count",
                "Speeches": "sum",
                "Dialogue Lines": "sum",
                "Unique Speakers": "mean"
            })
            .rename(columns={"Scene": "Scenes", "Unique Speakers": "Avg Unique Speakers"})
            .reset_index()
        )

        print("\nAct-Level Summary:")
        print(act_summary.to_string(index=False))

        layout_path = f"../csv/{safe_name}_layout.csv"
        layout_df.to_csv(layout_path, index=False)
        print(f"Saved detailed layout: {layout_path}\n")

    # -----------------------
    # Combine all play-level summaries
    # -----------------------
    combined_summary = pd.concat(all_play_summaries, ignore_index=True)
    combined_summary_path = "../csv/all_plays_story_stats.csv"
    combined_summary.to_csv(combined_summary_path, index=False)
    print(f"Saved combined story summary for all plays: {combined_summary_path}")

    return combined_summary
