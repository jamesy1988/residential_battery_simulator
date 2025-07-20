import logging
import xml.etree.ElementTree as ET
from typing import List


class Battery:
    def __init__(self, name: str, model: str, power: float, energy: float, current: float):
        self.name = name
        self.model = model
        self.power = power  # kW
        self.energy = energy  # kWh
        self.current = current  # Amps

        logging.debug(f"Initialized Battery: {self}")

    def __repr__(self):
        return (
            f"{self.name} ({self.model}): {self.power}kW, "
            f"{self.energy}kWh, {self.current}A"
        )


class BatteryLoader:
    def __init__(self, xml_path: str):
        self.xml_path = xml_path

    def load_batteries(self) -> List[Battery]:
        logging.info(f"Loading battery data from {self.xml_path}")
        batteries = []

        try:
            tree = ET.parse(self.xml_path)
            root = tree.getroot()
        except ET.ParseError as e:
            logging.error(f"Failed to parse XML file: {e}")
            return []

        for battery_elem in root.findall("battery"):
            try:
                def get_text(tag: str) -> str:
                    el = battery_elem.find(tag)
                    if el is None or el.text is None:
                        raise ValueError(f"Missing or empty tag: <{tag}>")
                    return el.text.strip()

                name = get_text("name")
                model = get_text("model")
                power = float(get_text("power"))
                energy = float(get_text("energy"))
                current = float(get_text("current"))

                battery = Battery(name, model, power, energy, current)
                batteries.append(battery)

            except Exception as e:
                logging.error(f"Error loading battery entry: {e}")
                logging.debug(f"Offending XML:\n{ET.tostring(battery_elem, encoding='unicode')}")

        logging.info(f"Successfully loaded {len(batteries)} battery definitions.")
        return batteries
